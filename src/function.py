import io
import logging
import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Union
from zipfile import ZipFile

from cognite.client.data_classes import DataSet, FileMetadata
from cognite.client.exceptions import CogniteAPIError
from cognite.experimental import CogniteClient
from cognite.experimental.data_classes import Function
from retry import retry

from config import DEPLOY_WAIT_TIME_SEC, FunctionConfig
from schedule import delete_all_schedules_for_ext_id

logger = logging.getLogger(__name__)


class FunctionDeployTimeout(Exception):
    pass


class FunctionDeployError(Exception):
    pass


def retrieve_dataset(client: CogniteClient, ext_id: str) -> DataSet:
    """
    Assuming internal IDs eventually will (read: should) die, we enforce the use
    of external IDs in this Github action... but since the SDK (cur 2.15.0)
    does not support data set external ID for FilesAPI, we need lookup...
    """
    try:
        ds = client.data_sets.retrieve(external_id=ext_id)
        if ds:
            return ds
        raise ValueError(f"No dataset exists with external ID: '{ext_id}'")

    except CogniteAPIError as exc:
        err_msg = "Unable to retrieve dataset: Deployment key is missing capability 'dataset:READ'."
        logger.error(err_msg)
        raise CogniteAPIError(err_msg, exc.code, exc.x_request_id) from None


def await_function_deployment(client: CogniteClient, external_id: str, wait_time_sec: int) -> Function:
    t0 = time.time()
    while time.time() <= t0 + wait_time_sec:
        function = client.functions.retrieve(external_id=external_id)
        if function is None:  # Should not ever happen... :shrug:
            err = f"No function with external_id={external_id} exists!"
            logger.warning(err)
            raise FunctionDeployError(err)
        elif function.status == "Ready":
            logger.info(f"Function deployment successful! Deployment took {time.time()-t0:.2f} seconds")
            return function
        elif function.status == "Failed":
            logger.warning(f"Deployment failed after {time.time()-t0:.2f} seconds! Error: {function.error['trace']}")
            raise FunctionDeployError(function.error["trace"])
        time.sleep(5)

    raise FunctionDeployTimeout(f"Function {external_id} did not deploy within {wait_time_sec} seconds.")


def try_delete(client: CogniteClient, external_id: str):
    try_delete_function(client, external_id)
    try_delete_function_file(client, get_file_name(external_id))
    # Schedules live on when functions die, so we always clean up:
    delete_all_schedules_for_ext_id(client, external_id)
    time.sleep(3)


def try_delete_function(client: CogniteClient, external_id: str):
    function = client.functions.retrieve(external_id=external_id)
    if function is not None:
        logger.info(f"Found existing function '{external_id}'. Deleting...")
        client.functions.delete(external_id=external_id)
        logger.info(f"- Delete of function '{external_id}' successful!")
    else:
        logger.info(f"Unable to delete function! External ID: '{external_id}' NOT found!")


def try_delete_function_file(client: CogniteClient, external_id: str):
    file_meta = client.files.retrieve(external_id=external_id)
    if file_meta is not None:
        logger.info(f"Found existing file {external_id}. Deleting...")
        try:
            client.files.delete(external_id=external_id)
            logger.info(f"- Delete of file '{external_id}' successful!")
        except CogniteAPIError:
            if file_meta.data_set_id is not None:
                logger.error(
                    "Unable to delete file! File is governed by data set with ID: {file_meta.data_set_id}. "
                    "Make sure your deployment credentials have write/owner access "
                    "(see README.md in function-action repo). Trying to ignore and continue as "
                    "this workflow will overwrite the file later."
                )
    else:
        logger.info(f"Unable to delete file! External ID: '{external_id}' NOT found!")


def create_function_and_wait(client: CogniteClient, file_id: int, config: FunctionConfig) -> Function:
    external_id, secrets = config.external_id, config.unpacked_secrets
    logger.info(f"Trying to create function '{external_id}'...")
    if secrets:
        logger.info(f"- Adding {len(secrets)} extra secret(s) to the function '{external_id}'")
    else:
        logger.info(f"- No extra secrets added to function '{external_id}'")
    client.functions.create(
        name=external_id,
        external_id=external_id,
        file_id=file_id,
        api_key=config.tenant.runtime_key,
        function_path=config.function_file,
        secrets=secrets,
        owner=config.owner,
        **config.get_memory_and_cpu(),  # Do not pass kwargs if mem/cpu is not set
    )
    logging.info(f"Function '{external_id}' created. Waiting for deployment...")
    return await_function_deployment(client, external_id, DEPLOY_WAIT_TIME_SEC)


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


def upload_zipped_code_to_files(client, file_bytes: bytes, name: str, ds: DataSet) -> FileMetadata:
    try:
        return client.files.upload_bytes(
            file_bytes,
            name=name,
            external_id=name,
            data_set_id=ds.id,
            overwrite=True,
        )
    except CogniteAPIError as exc:
        if ds.id is None:
            # Error is not dataset related, so we immediately re-raise
            raise
        if ds.write_protected:
            err_msg = (
                "Unable to upload file to WRITE-PROTECTED dataset: Deployment key MUST have capability "
                "'dataset:OWNER' (and have 'files:WRITE' scoped to the same dataset OR all files)."
            )
        else:
            err_msg = (
                "Unable to upload file to dataset: Deployment key must have capability "
                "'files:WRITE' scoped to the same dataset OR all files."
            )
        logger.error(err_msg)
        raise CogniteAPIError(err_msg, exc.code, exc.x_request_id) from None


def zip_and_upload_folder(client: CogniteClient, config: FunctionConfig, name: str) -> int:
    logger.info(f"Uploading code from '{config.function_folder}' to '{name}'")
    buf = io.BytesIO()  # TempDir, who needs that?! :rocket:
    with ZipFile(buf, mode="a") as zf:
        with temporary_chdir(config.function_folder):
            _write_files_to_zip_buffer(zf, directory=".")

        if config.common_folder is not None:
            with temporary_chdir(config.common_folder.parent):  # Note .parent
                logger.info(f"- Added common directory: '{config.common_folder}' to the file/function")
                _write_files_to_zip_buffer(zf, directory=config.common_folder)

    ds = DataSet(id=None)
    if config.data_set_external_id is not None:
        ds = retrieve_dataset(client, config.data_set_external_id)
        logger.info(
            f"- Using dataset '{ds.external_id}' to govern the file (has write protection: {ds.write_protected})."
        )
    else:
        logger.info("- No dataset will be used to govern the file!")

    file_meta = upload_zipped_code_to_files(client, buf.getvalue(), name, ds)
    if file_meta.id is not None:
        logger.info(f"- File uploaded successfully ({name})!")
        return file_meta.id
    raise FunctionDeployError(f"Failed to upload file ({name}) to CDF Files")


# Note: Do NOT catch CogniteNotFoundError (used in data set check, if it fails, it will always fail)
@retry(exceptions=(IOError, FunctionDeployTimeout, FunctionDeployError), tries=5, delay=2, jitter=2)
def upload_and_create(client: CogniteClient, config: FunctionConfig) -> Function:
    zip_file_name = get_file_name(config.external_id)  # Also external ID
    try_delete(client, config.external_id)
    try:
        file_id = zip_and_upload_folder(client, config, zip_file_name)
        return create_function_and_wait(client=client, file_id=file_id, config=config)
    except CogniteAPIError as e:
        if "Function externalId duplicated" in e.message:
            # Function was registered, but an unknown error occurred. Trigger retry:
            raise FunctionDeployError(e.message) from None
        raise  # We don't want to trigger retry for unknown problems


def get_file_name(function_name: str) -> str:
    # TODO: Kindly ask Cognite Functions to make this into a function...
    return function_name.replace("/", "-") + ".zip"  # forward-slash is not allowed in file names
