from pathlib import Path

import pytest
from cognite.client.testing import monkeypatch_cognite_client

from config import FunctionConfig, ScheduleConfig, TenantConfig

base_path = Path(__file__).parent / "test_files"


def test_read_config_whitespace_cron(valid_config):
    assert valid_config.schedules[0].cron == "* * * * *"
    assert valid_config.schedules[0].data.get("test_value") == 42


def test_cross_project_config(monkeypatch, loggedin_status):
    monkeypatch.setenv("FUNCTION_KEY", "FUNCTION_KEY")
    monkeypatch.setenv("DEPLOYMENT_KEY", "DEPLOYMENT_KEY")
    schedules = ["* * * * *"]
    with monkeypatch_cognite_client() as cdf_mock:
        cdf_mock.login.status.return_value = loggedin_status
        with pytest.raises(expected_exception=ValueError):
            FunctionConfig(
                external_id="test:hello_world_function",
                folder_path="hello_world_function",
                file="handler.py",
                tenant=TenantConfig(
                    cdf_project="demo",
                    deployment_key="DEPLOYMENT_KEY",
                    runtime_key="FUNCTION_KEY",
                    cdf_base_url="https://api.cognitedata.com",
                ),
                schedules=[
                    ScheduleConfig(
                        name=f"Schedule for test:hello_world_function #{i}",
                        cron=s,
                    )
                    for i, s in enumerate(schedules)
                ],
                remove_only=False,
            )
