import contextlib
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest
from cognite.client.data_classes import FileMetadata
from cognite.experimental.data_classes import Function

from src.function import (
    FunctionDeployError,
    FunctionDeployTimeout,
    await_function_deployment,
    create_and_wait,
    deploy_function,
    file_exists,
    function_exist,
    get_file_name,
    get_function_name,
    try_delete,
    try_delete_function,
    try_delete_function_file,
    upload_and_create,
    zip_and_upload_folder,
)


@pytest.fixture
def cognite_client_mock():
    return MagicMock()


@patch("src.function.shutil")
@patch("src.function.TemporaryDirectory")
def test_zip_and_upload_folder(temp_dir_mock, shutil_mock):
    mock_client = MagicMock()
    mock_client.files.upload.return_value = FileMetadata(id=1)
    temp_dir_mock.return_value.__enter__.return_value = "temp_dir"
    result = zip_and_upload_folder(mock_client, Path("path"), "filename")

    assert result == 1
    assert mock_client.files.upload.call_args_list == [
        call(
            "temp_dir/function.zip",
            name="filename",
            external_id="filename",
            overwrite=True,
        )
    ]
    assert shutil_mock.make_archive.call_args_list == [call("temp_dir/function", "zip", "path")]


@pytest.mark.parametrize(
    "retrieve_status, wait_time_seconds, expected, expectation",
    [
        (["Ready"], 1, True, contextlib.nullcontext()),
        (["Failed"], 1, False, pytest.raises(FunctionDeployError)),
        (["Not ready", "Ready"], 4, True, contextlib.nullcontext()),
        (["Not ready", "Failed"], 4, False, pytest.raises(FunctionDeployError)),
        (["Not ready"], 3, False, contextlib.nullcontext()),
    ],
)
def test_await_function_deployment(retrieve_status, wait_time_seconds, expected, expectation, cognite_client_mock):
    cognite_client_mock.functions.retrieve.side_effect = [
        Function(status=status, error={"trace": "some_error"}) for status in retrieve_status
    ]
    with expectation:
        assert expected == await_function_deployment(cognite_client_mock, "", wait_time_seconds)


@patch("src.function.try_delete_function")
@patch("src.function.try_delete_function_file")
@patch("src.function.get_file_name")
def test_try_delete(
    get_file_name_mock,
    try_delete_function_file_mock,
    try_delete_function,
    cognite_client_mock,
):
    file_name = "file/name"
    get_file_name_mock.return_value = "file_name.zip"
    try_delete(cognite_client_mock, file_name)

    assert try_delete_function.call_args_list == [call(cognite_client_mock, file_name)]
    assert get_file_name_mock.call_args_list == [call(file_name)]
    assert try_delete_function_file_mock.call_args_list == [call(cognite_client_mock, "file_name.zip")]


@pytest.mark.parametrize(
    "exists, expected_delete_calls",
    [(True, [call(external_id="some id")]), (False, [])],
)
@patch("src.function.function_exist")
def test_try_delete_function(functions_exist_mock, exists, expected_delete_calls, cognite_client_mock):
    functions_exist_mock.return_value = exists

    try_delete_function(cognite_client_mock, "some id")
    assert cognite_client_mock.functions.delete.call_args_list == expected_delete_calls


@pytest.mark.parametrize(
    "exists, expected_delete_calls",
    [(True, [call(external_id="some id")]), (False, [])],
)
@patch("src.function.file_exists")
def test_try_delete_function_file(file_exists_mock, cognite_client_mock, exists, expected_delete_calls):
    file_exists_mock.return_value = exists

    try_delete_function_file(cognite_client_mock, "some id")
    assert cognite_client_mock.files.delete.call_args_list == expected_delete_calls


@pytest.mark.parametrize(
    "success, expectation",
    [(True, contextlib.nullcontext()), (False, pytest.raises(FunctionDeployTimeout))],
)
@patch("src.function.await_function_deployment")
def test_create_and_wait(await_function_deployment_mock, success, expectation, cognite_client_mock):
    await_function_deployment_mock.return_value = success
    function = Function(external_id="id")
    cognite_client_mock.functions.create.return_value = function
    with expectation:
        assert function == create_and_wait(cognite_client_mock, "name", "id", Path("some path"), 1, "api key")
        assert await_function_deployment_mock.call_args_list == [call(cognite_client_mock, "id", 600)]


@patch("src.function.create_and_wait")
@patch("src.function.zip_and_upload_folder")
def test_upload_and_create(zip_and_upload_folder_mock, create_and_wait_mock, cognite_client_mock):
    function_name = "function/function"
    function_folder = Path("folder")
    function_path = Path("handler.py")
    file_id = 1

    zip_and_upload_folder_mock.return_value = file_id

    upload_and_create(cognite_client_mock, function_name, function_folder, function_path, "")
    assert zip_and_upload_folder_mock.call_args_list == [
        call(cognite_client_mock, function_folder, "function_function.zip")
    ]
    assert create_and_wait_mock.call_args_list == [
        call(
            cognite_client_mock,
            function_name,
            external_id=function_name,
            function_path=function_path,
            file_id=file_id,
            api_key="",
        )
    ]


@pytest.mark.parametrize("exception", [FunctionDeployTimeout, FunctionDeployError])
@patch("src.function.create_and_wait")
@patch("src.function.zip_and_upload_folder")
@patch("src.function.try_delete_function_file")
def test_upload_and_create_exception(
    try_delete_function_file_mock, zip_and_upload_folder_mock, create_and_wait_mock, cognite_client_mock, exception
):
    function_name = "function/function"
    function_folder = Path("folder")
    function_path = "handler.py"
    file_id = 1
    create_and_wait_mock.side_effect = exception
    zip_and_upload_folder_mock.return_value = file_id

    with pytest.raises(exception):
        upload_and_create(cognite_client_mock, function_name, function_folder, Path(function_path), "")
        assert zip_and_upload_folder_mock.call_args_list == [
            call(cognite_client_mock, function_folder, "function_function.zip")
        ]
        assert create_and_wait_mock.call_args_list == [
            call(
                cognite_client_mock,
                function_name,
                external_id=function_name,
                function_path=Path(function_path),
                file_id=file_id,
                api_key="",
            )
        ]
        assert try_delete_function_file_mock.call_args_list == [cognite_client_mock, "function_function.zip"]


@patch("src.function.try_delete")
@patch("src.function.upload_and_create")
@patch("src.function.get_function_name")
def test_deploy_function_push(get_function_name_mock, upload_and_create_mock, try_delete_mock, cognite_client_mock):
    function_name = "function"
    function_folder = "folder"
    function_path = "function path"
    expected_function = Function()

    get_function_name_mock.return_value = function_name
    upload_and_create_mock.return_value = expected_function

    result = deploy_function(cognite_client_mock, function_folder, function_path, "")
    assert result == expected_function
    assert try_delete_mock.call_args_list == [call(cognite_client_mock, function_name)]
    assert upload_and_create_mock.call_args_list == [
        call(
            cognite_client_mock,
            function_name,
            Path(function_folder),
            Path(function_path),
            "",
        )
    ]


@patch("src.function.try_delete")
@patch("src.function.upload_and_create")
@patch("src.function.get_function_name")
def test_deploy_function_delete(
    get_function_name_mock,
    upload_and_create_mock,
    try_delete_mock,
    monkeypatch,
    cognite_client_mock,
):
    function_name = "function"
    monkeypatch.setenv("DELETE_PR_FUNCTION", "True")
    get_function_name_mock.return_value = function_name

    assert deploy_function(cognite_client_mock, "", "some_path/handler.py", "", True) is None
    assert try_delete_mock.call_args_list == [call(cognite_client_mock, function_name)]
    assert upload_and_create_mock.call_args_list == []


@pytest.mark.parametrize("response, expected", [(Function(), True), (None, False)])
def test_function_exist(response, expected, cognite_client_mock):
    cognite_client_mock.functions.retrieve.return_value = response
    assert function_exist(cognite_client_mock, "") == expected


@pytest.mark.parametrize("response, expected", [(FileMetadata(), True), (None, False)])
def test_file_exist(response, expected, cognite_client_mock):
    cognite_client_mock.files.retrieve.return_value = response
    assert file_exists(cognite_client_mock, "") == expected


@pytest.mark.parametrize("is_pr, expected_name_postfix", [(True, "/head_ref"), (False, ":latest")])
def test_get_function_name(is_pr, expected_name_postfix, monkeypatch):
    monkeypatch.setenv("GITHUB_REPOSITORY", "test_repo")
    monkeypatch.setenv("GITHUB_HEAD_REF", "head_ref")

    expected_name_prefix = "test_repo/f1/f2/my_handler.py"
    function_folder = Path("f1")
    function_path = Path("f2/my_handler.py")

    assert get_function_name(function_folder, function_path, is_pr) == f"{expected_name_prefix}{expected_name_postfix}"


@pytest.mark.parametrize(
    "function_name, file_name",
    [("my file 1", "my file 1.zip"), ("my/file/1", "my_file_1.zip")],
)
def test_get_file_name(function_name, file_name):
    assert get_file_name(function_name) == file_name
