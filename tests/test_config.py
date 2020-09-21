import contextlib
from pathlib import Path
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from src.config import Config, FunctionConfig, InvalidCronException, get_config, read_config

base_path = Path(__file__).parent / "test_files"


@pytest.mark.parametrize(
    "file_path, expectation",
    [
        (base_path / "config1.yaml", contextlib.nullcontext()),  # Valid config
        (base_path / "config2.yaml", pytest.raises(ValueError)),  # Empty config
        (
            base_path / "config3.yaml",
            pytest.raises(ValidationError),
        ),  # Invalid config, missing fields
        (base_path / "config4.yaml", pytest.raises(InvalidCronException)),  # Invalid cron
        (
            base_path / "config5.yaml",
            pytest.raises(ValidationError),
        ),  # Non existing keys
        (base_path / "config6.yaml", contextlib.nullcontext()),  # Valid config
    ],
)
def test_read_config(file_path: Path, expectation, monkeypatch):
    monkeypatch.setenv("FUNCTION_KEY", "function_key")
    monkeypatch.setenv("DEPLOYMENT_KEY", "deployment_key")
    with expectation:
        read_config(file_path)


def test_read_config_whitespace_cron(monkeypatch):
    monkeypatch.setenv("FUNCTION_KEY", "function_key")
    monkeypatch.setenv("DEPLOYMENT_KEY", "deployment_key")
    config = read_config(base_path / "config7.yaml")

    assert config.functions["function1"].schedules[0].cron == "* * * * *"


@patch("src.config.read_config")
def test_get_config(read_config_mock):
    function_config = FunctionConfig(folder_path="", file=".py", tenants=[])

    config = Config(functions={"function 1": function_config})

    read_config_mock.return_value = config
    assert get_config(Path(), "function 1") == function_config

    with pytest.raises(KeyError):
        get_config(Path(), "function 2")
