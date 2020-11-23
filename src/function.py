import io
import logging
import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Union
from zipfile import ZipFile

from cognite.client.exceptions import CogniteNotFoundError
from cognite.experimental import CogniteClient
from cognite.experimental.data_classes import Function
from retry import retry

from config import FunctionConfig

logger = logging.getLogger(__name__)


class FunctionDeployTimeout(Exception):
    pass


class FunctionDeployError(Exception):
    pass


def await_function_deployment(client: CogniteClient, external_id: str, wait_time_sec: int) -> Function:
    t_end = time.time() + wait_time_sec
    while time.time() <= t_end:
        function = client.functions.retrieve(external_id=external_id)
        if function is not None:
            if function.status == "Ready":
                logger.info(f"Deployment took {round(t_end-time.time(), 2)} seconds")
                return function
            if function.status == "Failed":
                raise FunctionDeployError(function.error["trace"])
        time.sleep(3)

    raise FunctionDeployTimeout(f"Function {external_id} did not deploy within {wait_time_sec} seconds.")


def try_delete(client: CogniteClient, external_id: str):
    try_delete_function(client, external_id)
    try_delete_function_file(client, get_file_name(external_id))


def try_delete_function(client: CogniteClient, external_id: str):
    if function_exist(client, external_id):  # I don't want to deal with mocks for now :(
        function = client.functions.retrieve(external_id=external_id)
        if function is not None:
            for schedule in function.list_schedules():
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


def create_function_and_wait(client: CogniteClient, file_id: int, config: FunctionConfig) -> Function:
    external_id = config.external_id
    logger.info(f"Trying to create function '{external_id}'...")
    client.functions.create(
        name=external_id,
        external_id=external_id,
        file_id=file_id,
        api_key=config.tenant.runtime_key,
        function_path=config.file,
        secrets=config.unpacked_secrets,
    )
    logging.info(f"Function '{external_id}' creating. Waiting for deployment...")
    function = await_function_deployment(client, external_id, config.deploy_wait_time_sec)
    logging.info(f"Function '{external_id}' deployed successfully!")
    return function


@contextmanager
def temporary_chdir(path: Union[str, Path]):
    old_path = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(old_path)


def _write_files_to_zip_buffer(zf: ZipFile, directory: Path):
    for dirpath, _, files in os.walk(directory):
        zf.write(dirpath)
        for f in files:
            zf.write(Path(dirpath) / f)


def zip_and_upload_folder(client: CogniteClient, config: FunctionConfig, name: str) -> int:
    logger.info(f"Uploading code from '{config.folder_path}' to '{name}'")
    buf = io.BytesIO()  # TempDir, who needs that?! :rocket:
    with ZipFile(buf, mode="a") as zf:
        with temporary_chdir(config.folder_path):
            _write_files_to_zip_buffer(zf, directory=".")

        if config.common_folder_path is not None:
            with temporary_chdir(config.common_folder_path.parent):  # Note .parent
                logger.info(f"Added common directory: '{config.common_folder_path}' to the function")
                _write_files_to_zip_buffer(zf, directory=config.common_folder_path)

    file_meta = client.files.upload_bytes(buf.getvalue(), name=name, external_id=name)
    if file_meta.id is not None:
        logger.info("Upload successful!")
        return file_meta.id
    raise FunctionDeployError(f"Failed to upload file ({name}) to CDF Files")


@retry(exceptions=(IOError, FunctionDeployTimeout, FunctionDeployError), tries=5, delay=2, jitter=2)
def upload_and_create(client: CogniteClient, config: FunctionConfig) -> Function:
    zip_file_name = get_file_name(config.external_id)  # Also external ID

    if config.overwrite:
        # upsert was requested. delete schedules, function and files
        try_delete(client=client, external_id=config.external_id)

    try:
        file_id = zip_and_upload_folder(client, config, zip_file_name)
        return create_function_and_wait(client=client, file_id=file_id, config=config)

    except (FunctionDeployError, FunctionDeployTimeout):
        try_delete_function_file(client, zip_file_name)
        raise


def function_exist(client: CogniteClient, external_id: str) -> bool:
    return client.functions.retrieve(external_id=external_id) is not None


def file_exists(client: CogniteClient, external_id: str) -> bool:
    return client.files.retrieve(external_id=external_id) is not None


def get_file_name(function_name: str) -> str:
    # TODO: Kindly ask Cognite Functions to make this into a function...
    return function_name.replace("/", "-") + ".zip"  # forward-slash is not allowed in file names
