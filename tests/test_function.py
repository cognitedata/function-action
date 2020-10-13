import contextlib
from unittest.mock import call, patch, MagicMock

import pytest
from cognite.client.data_classes import FileMetadata
from cognite.experimental.data_classes import Function

from function import (
    FunctionDeployError,
    FunctionDeployTimeout,
    await_function_deployment,
    create_and_wait,
    deploy_function,
    file_exists,
    function_exist,
    get_file_name,
    try_delete,
    try_delete_function,
    try_delete_function_file,
    upload_and_create,
)


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
def test_await_function_deployment(
    retrieve_status, wait_time_seconds, expected, expectation, cognite_experimental_client_mock
):
    responses = [Function(status=status, error={"trace": "some_error"}) for status in retrieve_status]
    cognite_experimental_client_mock.functions.retrieve.side_effect = responses
    with expectation:
        r = await_function_deployment(cognite_experimental_client_mock, "", wait_time_seconds)
        if expected:
            assert r
            assert r == responses[-1]
        else:
            assert r is None


@patch("function.try_delete_function")
@patch("function.try_delete_function_file")
def test_try_delete(
    try_delete_function_file_mock,
    try_delete_function,
    cognite_client_mock,
):
    file_name = "file/external_id"
    try_delete(cognite_client_mock, file_name)

    assert try_delete_function.call_args_list == [call(cognite_client_mock, file_name)]
    assert try_delete_function_file_mock.call_args_list == [call(cognite_client_mock, "file-external_id.zip")]


@pytest.mark.parametrize(
    "exists, expected_delete_calls",
    [(True, [call(external_id="some id")]), (False, [])],
)
@patch("function.function_exist")
def test_try_delete_function(functions_exist_mock, exists, expected_delete_calls, cognite_experimental_client_mock):
    functions_exist_mock.return_value = exists

    try_delete_function(cognite_experimental_client_mock, "some id")
    assert cognite_experimental_client_mock.functions.delete.call_args_list == expected_delete_calls


@pytest.mark.parametrize(
    "exists, expected_delete_calls",
    [(True, [call(external_id="some id")]), (False, [])],
)
@patch("function.file_exists")
def test_try_delete_function_file(file_exists_mock, cognite_client_mock, exists, expected_delete_calls):
    file_exists_mock.return_value = exists

    try_delete_function_file(cognite_client_mock, "some id")
    assert cognite_client_mock.files.delete.call_args_list == expected_delete_calls


@pytest.mark.parametrize(
    "response, expectation",
    [(Function(external_id="id", id=1), contextlib.nullcontext()), (None, pytest.raises(FunctionDeployTimeout))],
)
@patch("function.await_function_deployment")
def test_create_and_wait(await_function_deployment_mock, response, expectation, cognite_experimental_client_mock):
    await_function_deployment_mock.return_value = response
    function = Function(external_id="id")
    cognite_experimental_client_mock.functions.create.return_value = function
    mock_config = MagicMock()
    mock_config.deploy_wait_time_sec = 1337
    with expectation:
        assert response == create_and_wait(cognite_experimental_client_mock, "id", mock_config)
        print(await_function_deployment_mock.call_args_list)
        assert await_function_deployment_mock.call_args_list == [call(cognite_experimental_client_mock, "id", 1337)]


@pytest.mark.parametrize("exception", [FunctionDeployTimeout, FunctionDeployError])
@patch("function.create_and_wait")
@patch("function.try_delete_function_file")
def test_upload_and_create_exception(
    try_delete_function_file_mock,
    create_and_wait_mock,
    cognite_client_mock,
    exception,
    valid_config,
):
    create_and_wait_mock.side_effect = exception

    with pytest.raises(exception):
        upload_and_create(cognite_client_mock, valid_config)


@patch("function.try_delete")
@patch("function.upload_and_create")
def test_deploy_function_push(
    upload_and_create_mock, try_delete_mock, cognite_client_mock, valid_config
):
    expected_function = Function()
    upload_and_create_mock.return_value = expected_function

    result = deploy_function(cognite_client_mock, valid_config)
    assert result == expected_function
    assert try_delete_mock.call_args_list == [call(cognite_client_mock, valid_config.external_id)]
    assert upload_and_create_mock.call_args_list == [call(cognite_client_mock, valid_config)]


@patch("function.try_delete")
@patch("function.upload_and_create")
def test_function_delete(
    upload_and_create_mock, try_delete_mock, monkeypatch, cognite_client_mock, valid_config
):
    valid_config.remove_only = True
    assert deploy_function(cognite_client_mock, valid_config) is None
    assert try_delete_mock.call_args_list == [call(cognite_client_mock, valid_config.external_id)]
    assert upload_and_create_mock.call_args_list == []


@pytest.mark.parametrize("response, expected", [(Function(), True), (None, False)])
def test_function_exist(response, expected, cognite_experimental_client_mock):
    cognite_experimental_client_mock.functions.retrieve.return_value = response
    assert function_exist(cognite_experimental_client_mock, "") == expected


@pytest.mark.parametrize("response, expected", [(FileMetadata(), True), (None, False)])
def test_file_exist(response, expected, cognite_client_mock):
    cognite_client_mock.files.retrieve.return_value = response
    assert file_exists(cognite_client_mock, "") == expected


@pytest.mark.parametrize(
    "function_name, file_name",
    [("my file 1", "my file 1.zip"), ("my/file/1", "my-file-1.zip")],
)
def test_get_file_name(function_name, file_name):
    assert get_file_name(function_name) == file_name
