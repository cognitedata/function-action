import os
import shutil
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional

from cognite.experimental import CogniteClient
from cognite.experimental.data_classes import Function


class FunctionDeployTimeout(Exception):
    pass


class FunctionDeployError(Exception):
    pass


def zip_and_upload_folder(client: CogniteClient, folder: Path, file_name: str) -> int:
    print(f"Uploading code from {folder} to {file_name}")
    with TemporaryDirectory() as tmpdir:
        zip_path = Path(tmpdir) / "function"
        shutil.make_archive(str(zip_path), "zip", str(folder))
        file = client.files.upload(f"{zip_path}.zip", name=file_name, external_id=file_name, overwrite=True)
        print(f"Upload complete.")
        return file.id


def await_function_deployment(client: CogniteClient, external_id: str, wait_time_seconds: int):
    t_end = time.time() + wait_time_seconds
    while time.time() < t_end:
        function = client.functions.retrieve(external_id=external_id)
        if function.status == "Ready":
            return True
        if function.status == "Failed":
            raise FunctionDeployError(function.error["trace"])
        time.sleep(3.0)

    return False


def try_delete(client: CogniteClient, name: str):
    try_delete_function(client, name)
    try_delete_function_file(client, get_file_name(name))


def try_delete_function(client: CogniteClient, external_id: str):
    if function_exist(client, external_id):
        print(f"Found existing function {external_id}. Deleting ...")
        client.functions.delete(external_id=external_id)
        print(f"Did delete function {external_id}.")


def try_delete_function_file(client: CogniteClient, external_id: str):
    if file_exists(client, external_id):
        print(f"Found existing file {external_id}. Deleting ...")
        client.files.delete(external_id=external_id)
        print(f"Did delete file {external_id}.")


def create_and_wait(
    client: CogniteClient,
    name: str,
    external_id: str,
    function_path: Path,
    file_id: int,
    api_key: str,
):
    print(f"Will create function {external_id}. With api key: {api_key is not None}")
    function = client.functions.create(
        name=name,
        external_id=external_id,
        file_id=file_id,
        api_key=api_key,
        function_path=str(function_path),
    )
    print(f"Created function {external_id}. Waiting for deployment ...")
    wait_time_seconds = 600  # 10 minutes
    deployed = await_function_deployment(client, function.external_id, wait_time_seconds)
    if not deployed:
        print(f"Function {external_id} did not deploy within {wait_time_seconds} seconds.")
        raise FunctionDeployTimeout(f"Function {external_id} did not deploy within {wait_time_seconds} seconds.")
    print(f"Function {external_id} is deployed.")
    return function


def upload_and_create(client: CogniteClient, name: str, folder: Path, function_path: Path, api_key: str) -> Function:
    file_name = get_file_name(name)
    file_id = zip_and_upload_folder(client, folder, file_name)
    try:
        return create_and_wait(
            client,
            name,
            external_id=name,
            function_path=function_path,
            file_id=file_id,
            api_key=api_key,
        )
    except (FunctionDeployError, FunctionDeployTimeout) as e:
        try_delete_function_file(client, file_name)
        raise e


def deploy_function(
    client: CogniteClient,
    function_folder: str,
    function_path: str,
    api_key: str,
    is_pr: bool = False,
    is_delete: bool = False,
) -> Optional[Function]:

    folder_path = Path(function_folder)
    function_path = Path(function_path)
    function_name = get_function_name(folder_path, is_pr)
    try_delete(client, function_name)  # Delete old function and file

    if is_pr and is_delete:
        return

    # Upload file and create function
    function = upload_and_create(client, function_name, folder_path, function_path, api_key)
    print(f"Successfully created and deployed function {function_name} with id {function.id}")

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
