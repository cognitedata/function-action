import pytest
from cognite.client.testing import monkeypatch_cognite_client

from config import FunctionConfig


def test_read_config_whitespace_cron(valid_config):
    assert valid_config.schedules[0].cron == "* * * * *"
    assert valid_config.schedules[0].data["test_value"] == 42
    assert len(valid_config.unpacked_secrets) == 1
    assert valid_config.unpacked_secrets["key"] == "value"


def test_bad_config__cdf_project_mismatch(loggedin_status, valid_config_dct):
    valid_config_dct["tenant"]["cdf_project"] = "not mock!!"
    with monkeypatch_cognite_client() as cdf_mock:
        cdf_mock.login.status.return_value = loggedin_status
        with pytest.raises(ValueError):
            _ = FunctionConfig.parse_obj(valid_config_dct)
