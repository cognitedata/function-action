import logging
import os
import time
from pathlib import Path
from typing import Optional

from cognite.experimental import CogniteClient
from cognite.experimental.data_classes import Function
from retry import retry

from config import FunctionConfig

logger = logging.getLogger(__name__)


class FunctionDeployTimeout(Exception):
    pass


class FunctionDeployError(Exception):
    pass


def await_function_deployment(client: CogniteClient, external_id: str, wait_time_sec: int) -> Optional[Function]:
    t_end = time.time() + wait_time_sec
    while time.time() <= t_end:
        function: Optional[Function] = client.functions.retrieve(external_id=external_id)
        if function is not None:
            if function.status == "Ready":
                return function
            if function.status == "Failed":
                raise FunctionDeployError(function.error["trace"])
        time.sleep(3)

    return None


def try_delete(client: CogniteClient, external_id: str):
    try_delete_function(client, external_id)
    try_delete_function_file(client, get_file_name(external_id))


def try_delete_function(client: CogniteClient, external_id: str):
    if function_exist(client, external_id):  # I don't want to deal with mocks for now :(
        func = client.functions.retrieve(external_id=external_id)
        if func is not None:
            for schedule in func.list_schedules():
                # We want to delete ALL schedules since we don't keep state anywhere
                # 1. Those we are going to recreate
                # 2. Those removed permanently
                client.functions.schedules.delete(schedule.id)

            logger.info(f"Found existing function '{external_id}'. Deleting ...")
            client.functions.delete(external_id=external_id)
            logger.info(f"Delete of function '{external_id}' successful!")


def try_delete_function_file(client: CogniteClient, external_id: str):
    if file_exists(client, external_id):
        logger.info(f"Found existing file {external_id}. Deleting ...")
        client.files.delete(external_id=external_id)
        logger.info(f"Did delete file {external_id}.")


def create_and_wait(client: CogniteClient, external_id: str, config: FunctionConfig):
    logger.info(f"Will create function {external_id}")
    client.functions.create(
        name=external_id,
        external_id=external_id,
        folder=config.folder_path,
        api_key=config.tenant.runtime_key,
        function_path=config.file,
    )
    logging.info(f"Created function {external_id}. Waiting for deployment...")
    wait_time_sec = config.deploy_wait_time_sec
    deployed = await_function_deployment(client, external_id, wait_time_sec)
    if deployed is None:
        raise FunctionDeployTimeout(f"Function {external_id} did not deploy within {wait_time_sec} seconds.")
    logging.info(f"Function {external_id} is deployed.")
    return deployed


@retry(exceptions=(FunctionDeployTimeout, FunctionDeployError), tries=5, delay=2, jitter=2)
def upload_and_create(client: CogniteClient, config: FunctionConfig) -> Function:
    try:
        return create_and_wait(client=client, external_id=config.external_id, config=config)
    except (FunctionDeployError, FunctionDeployTimeout):
        try_delete_function_file(client, get_file_name(config.external_id))
        raise


def deploy_function(client: CogniteClient, config: FunctionConfig) -> Optional[Function]:
    try_delete(client, config.external_id)  # Delete old function and file

    function = None
    if not config.remove_only:
        # Upload file and create function
        function = upload_and_create(client, config)
        logger.info(f"Successfully created and deployed function {config.external_id} with id {function.id}")

    return function


def function_exist(client: CogniteClient, external_id: str) -> bool:
    return client.functions.retrieve(external_id=external_id) is not None


def file_exists(client: CogniteClient, external_id: str) -> bool:
    return client.files.retrieve(external_id=external_id) is not None


def get_function_name(function_folder: Path, is_pr: bool) -> str:
    github_repo = os.environ["GITHUB_REPOSITORY"]
    github_head_ref = os.environ["GITHUB_HEAD_REF"]
    full_path = Path(github_repo) / "" if function_folder == "." else function_folder
    return full_path + (f"/{github_head_ref}" if is_pr else ":latest")


def get_file_name(function_name: str) -> str:
    return function_name.replace("/", "-") + ".zip"  # / not allowed in file names
