from contextlib import contextmanager
from unittest import mock
from unittest.mock import MagicMock

import pytest
from cognite.client.data_classes import LoginStatus
from cognite.client.testing import monkeypatch_cognite_client
from cognite.experimental import CogniteClient
from cognite.experimental._api.functions import FunctionsAPI, FunctionSchedulesAPI

from config import FunctionConfig, ScheduleConfig, TenantConfig


class ExperimentalCogniteClientMock(MagicMock):
    """Mock for experimental.CogniteClient object"""

    def __init__(self, *args, **kwargs):
        if "parent" in kwargs:
            super().__init__(*args, **kwargs)
            return
        super().__init__(spec=CogniteClient, *args, **kwargs)
        self.functions = MagicMock(spec=FunctionsAPI)
        self.functions.schedules = MagicMock(spec_set=FunctionSchedulesAPI)


@contextmanager
def monkeypatch_cognite_experimental_client():
    cognite_client_mock = ExperimentalCogniteClientMock()
    CogniteClient.__new__ = lambda *args, **kwargs: cognite_client_mock
    yield cognite_client_mock
    CogniteClient.__new__ = lambda cls, *args, **kwargs: super(CogniteClient, cls).__new__(cls)


@pytest.fixture
def loggedin_status():
    return LoginStatus(user="mock", project="mock", logged_in=True, project_id=-1, api_key_id=-1)


@pytest.fixture
def valid_config(monkeypatch, loggedin_status):
    monkeypatch.setenv("FUNCTION_KEY", "FUNCTION_KEY")
    monkeypatch.setenv("DEPLOYMENT_KEY", "DEPLOYMENT_KEY")
    with monkeypatch_cognite_client() as cdf_mock:
        cdf_mock.login.status.return_value = loggedin_status
        return FunctionConfig(
            external_id="test:hello_world_function/function",
            folder_path="tests",
            file="handler.py",
            tenant=TenantConfig(
                cdf_project="mock",
                deployment_key="DEPLOYMENT_KEY",
                runtime_key="FUNCTION_KEY",
                cdf_base_url="https://api.cognitedata.com",
            ),
            schedule_file="configs/valid_schedule.yml",
            remove_only=False,
        )


@pytest.fixture
def cognite_experimental_client_mock():
    with monkeypatch_cognite_experimental_client() as client:
        yield client


@pytest.fixture
def cognite_client_mock():
    with monkeypatch_cognite_client() as client:
        yield client
