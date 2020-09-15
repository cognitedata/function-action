import contextlib
from pathlib import Path
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from src.config import get_config, read_config

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
        (base_path / "config4.yaml", pytest.raises(ValidationError)),  # Invalid cron
        (
            base_path / "config5.yaml",
            pytest.raises(ValidationError),
        ),  # Non existing keys
        (base_path / "config6.yaml", contextlib.nullcontext()),  # Valid config
        (base_path / "config7.yaml", contextlib.nullcontext()),  # Valid config
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

    assert config.functions["function1"].schedule.cron == "* * * * *"


test_config = {"functions": {"function 1": {}, "function 2": {}}}


@pytest.mark.parametrize(
    "config, expected, function_name, expectation",
    [
        (test_config, test_config, "", contextlib.nullcontext()),
        (
            test_config,
            {"functions": {"function 1": {}}},
            "function 1",
            contextlib.nullcontext(),
        ),
        (test_config, {}, "function 2", pytest.raises(KeyError)),
    ],
)
@patch("src.config.read_config")
def test_get_config(read_config_mock, config, expected, function_name, expectation):
    read_config_mock.return_value = config
    with expectation:
        assert get_config(Path(), function_name) == expected
