import contextlib
from unittest.mock import MagicMock, call, patch

import pytest
from cognite.experimental.data_classes import Function

from config import DEPLOY_WAIT_TIME_SEC
from function import (
    FunctionDeployError,
    FunctionDeployTimeout,
    await_function_deployment,
    create_function_and_wait,
    delete_function,
    delete_single_cognite_function,
    get_file_name,
    upload_and_create_function,
)

# TODO: Tests need an overhaul / update


@pytest.mark.parametrize(
    "retrieve_status, wait_time_seconds, expectation",
    [
        (["Ready"], 1, contextlib.nullcontext()),
        (["Failed"], 1, pytest.raises(FunctionDeployError)),
        (["Not ready", "Ready"], 6, contextlib.nullcontext()),
        (["Not ready", "Failed"], 6, pytest.raises(FunctionDeployError)),
    ],
)
def test_await_function_deployment(retrieve_status, wait_time_seconds, expectation, cognite_experimental_client_mock):
    responses = [Function(status=status, error={"trace": "some_error"}) for status in retrieve_status]
    cognite_experimental_client_mock.functions.retrieve.side_effect = responses
    with expectation:
        r = await_function_deployment(cognite_experimental_client_mock, "", wait_time_seconds)
        assert isinstance(r, Function)
        assert r == responses[-1]


@patch("function.delete_function")
@patch("function.delete_function_file")
def test_delete_single_cognite_function(
    delete_function_file_mock,
    delete_function,
    cognite_experimental_client_mock,
):
    file_name = "file/external_id"
    delete_single_cognite_function(cognite_experimental_client_mock, file_name)

    assert delete_function.call_args_list == [call(cognite_experimental_client_mock, file_name)]
    assert delete_function_file_mock.call_args_list == [call(cognite_experimental_client_mock, "file-external_id.zip")]


@pytest.mark.parametrize(
    "function, expected_delete_calls",
    [(Function(id=123), [call(external_id="some id")]), (None, [])],
)
def test_delete_function(function, expected_delete_calls, cognite_experimental_client_mock):
    cognite_experimental_client_mock.functions.retrieve.return_value = function
    delete_function(cognite_experimental_client_mock, "some id")
    assert cognite_experimental_client_mock.functions.delete.call_args_list == expected_delete_calls


@pytest.mark.parametrize(
    "response, expectation",
    [(Function(external_id="id", id=1), contextlib.nullcontext())],
)
@patch("function.await_function_deployment")
def test_create_and_wait(await_function_deployment_mock, response, expectation, cognite_experimental_client_mock):
    await_function_deployment_mock.return_value = response
    mock_external_id = response.external_id
    cognite_experimental_client_mock.functions.create.return_value = Function(external_id=mock_external_id)
    mock_config = MagicMock()
    mock_config.external_id = mock_external_id
    with expectation:
        assert response == create_function_and_wait(cognite_experimental_client_mock, mock_external_id, mock_config)
        assert await_function_deployment_mock.call_args_list == [
            call(cognite_experimental_client_mock, mock_external_id, DEPLOY_WAIT_TIME_SEC)
        ]


@pytest.mark.parametrize("exception", [FunctionDeployTimeout, FunctionDeployError])
@patch("function.create_function_and_wait")
@patch("function.delete_function_file")
def test_upload_and_create_function_exception(
    delete_function_file_mock,
    create_and_wait_mock,
    cognite_client_mock,
    cognite_experimental_client_mock,
    exception,
    valid_config,
):
    create_and_wait_mock.side_effect = exception
    cognite_client_mock.functions = cognite_experimental_client_mock.functions

    with pytest.raises(exception):
        upload_and_create_function(cognite_client_mock, valid_config)


@pytest.mark.parametrize(
    "function_name, file_name",
    [("my file 1", "my file 1.zip"), ("my/file/1", "my-file-1.zip")],
)
def test_get_file_name(function_name, file_name):
    assert get_file_name(function_name) == file_name
