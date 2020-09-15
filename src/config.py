import os
import re
from pathlib import Path
from typing import Dict, List, Optional

from crontab import CronSlices
from pydantic import BaseModel, validator
from ruamel.yaml import YAML


class TenantConfig(BaseModel):
    cdf_project: str
    deployment_key_name: str
    function_key_name: str
    cdf_base_url: Optional[str]

    @validator("deployment_key_name", "function_key_name")
    def key_exists(cls, key_name):
        if not os.getenv(key_name):
            raise ValueError(f"Environment variable named {key_name} not set")


class ScheduleConfig(BaseModel):
    cron: str
    name: str

    @validator("cron")
    def valid_cron(cls, v):
        if not CronSlices.is_valid(v):
            raise ValueError(f"Invalid cron expression: '{v}'")

        return v.strip()


class FunctionConfig(BaseModel):
    path: str
    schedule: Optional[ScheduleConfig]
    tenants: List[TenantConfig]


class Config(BaseModel):
    functions: Dict[str, FunctionConfig]
    function_folder: str


def read_config(file_path: Path) -> Config:
    content = YAML(typ="safe").load(file_path.open().read())
    if not content:
        raise ValueError(f"Expected to find content in file {file_path}, but was empty")
    return Config(**content)


def get_config(file_path: Path, function_name: str = "") -> Config:
    config = read_config(file_path)
    if function_name:
        config["functions"] = {function_name: config["functions"][function_name]}

    return config
