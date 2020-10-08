import logging
import os
import shutil
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional

from cognite.client.data_classes import FileMetadata
from cognite.experimental import CogniteClient
from cognite.experimental.data_classes import Function

from config import FunctionConfig


class FunctionDeployTimeout(Exception):
    pass


class FunctionDeployError(Exception):
    pass


logger = logging.getLogger(__name__)


def zip_and_upload_folder(client: CogniteClient, folder: Path, file_name: str) -> int:
    logger.info(f"Uploading code from {folder} to {file_name}")
    with TemporaryDirectory() as tmpdir:
        zip_path = Path(tmpdir) / "function"
        shutil.make_archive(str(zip_path), "zip", str(folder))
        file: FileMetadata = client.files.upload(
            f"{zip_path}.zip", name=file_name, external_id=file_name, overwrite=True
        )
        logger.info(f"Upload complete.")
        if file.id is not None:
            return file.id
    raise FunctionDeployError("Failed to upload file to Cognite Data Fusion")


def await_function_deployment(client: CogniteClient, external_id: str, wait_time_seconds: int):
    t_end = time.time() + wait_time_seconds
    while time.time() < t_end:
        function: Optional[Function] = client.functions.retrieve(external_id=external_id)
        if function is not None:
            if function.status == "Ready":
                return True
            if function.status == "Failed":
                raise FunctionDeployError(function.error["trace"])
        time.sleep(3.0)

    return False


def try_delete(client: CogniteClient, external_id: str):
    try_delete_function(client, external_id)
    try_delete_function_file(client, get_file_name(external_id))


def try_delete_function(client: CogniteClient, external_id: str):
    if function_exist(client, external_id):  # I don't want to deal with mocks for now :(
        func = client.functions.retrieve(external_id=external_id)
        if func is not None:
            for schedule in func.list_schedules():
                # we want to delete All schedules since we don't keep state anywhere
                # 1. those we are going to recreate
                # 2. those removed permanently
                client.functions.schedules.delete(schedule.id)

            logger.info(f"Found existing function {external_id}. Deleting ...")
            client.functions.delete(external_id=external_id)
            logger.info(f"Did delete function {external_id}.")


def try_delete_function_file(client: CogniteClient, external_id: str):
    if file_exists(client, external_id):
        logger.info(f"Found existing file {external_id}. Deleting ...")
        client.files.delete(external_id=external_id)
        logger.info(f"Did delete file {external_id}.")


def create_and_wait(
    client: CogniteClient,
    name: str,
    external_id: str,
    function_path: str,
    file_id: int,
    api_key: str,
):
    logger.info(f"Will create function {external_id}")
    function: Function = client.functions.create(
        name=name,
        external_id=external_id,
        file_id=file_id,
        api_key=api_key,
        function_path=function_path,
    )
    logger.info(f"Created function {external_id}. Waiting for deployment ...")
    wait_time_seconds = 600  # 10 minutes
    deployed = await_function_deployment(client, function.external_id, wait_time_seconds)
    if not deployed:
        logger.error(f"Function {external_id} did not deploy within {wait_time_seconds} seconds.")
        raise FunctionDeployTimeout(f"Function {external_id} did not deploy within {wait_time_seconds} seconds.")
    logger.info(f"Function {external_id} is deployed.")
    return function


def upload_and_create(client: CogniteClient, config: FunctionConfig) -> Function:
    zip_file_name = get_file_name(config.external_id)
    file_id = zip_and_upload_folder(client, Path(config.folder_path), zip_file_name)
    try:
        return create_and_wait(
            client,
            config.external_id,
            external_id=config.external_id,
            function_path=config.file,
            file_id=file_id,
            api_key=config.tenant.runtime_key,
        )
    except (FunctionDeployError, FunctionDeployTimeout) as e:
        try_delete_function_file(client, zip_file_name)
        raise e


def deploy_function(client: CogniteClient, config: FunctionConfig) -> Optional[Function]:
    try_delete(client, config.external_id)  # Delete old function and file

    function = None
    if not config.remove_only:
        # Upload file and create function
        function = upload_and_create(client, config)
        logger.info(f"Successfully created and deployed function {config.external_id} with id {function.id}")

    return function


def function_exist(client: CogniteClient, external_id: str) -> bool:
    return bool(client.functions.retrieve(external_id=external_id))


def file_exists(client: CogniteClient, external_id: str) -> bool:
    return bool(client.files.retrieve(external_id=external_id))


def get_function_name(function_folder: Path, is_pr: bool) -> str:
    github_repo = os.environ["GITHUB_REPOSITORY"]
    github_head_ref = os.environ["GITHUB_HEAD_REF"]
    full_path = Path(github_repo) / f"{'' if function_folder == '.' else function_folder}"
    name = f"{full_path}{f'/{github_head_ref}' if is_pr else ':latest'}"

    return name


def get_file_name(function_name: str) -> str:
    return function_name.replace("/", "_") + ".zip"  # / not allowed in file names
