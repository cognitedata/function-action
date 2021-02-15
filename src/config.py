import base64
import contextlib
import json
import warnings
from pathlib import Path
from typing import Dict, List, Optional

import yaml
from cognite.client import CogniteClient
from cognite.experimental import CogniteClient as ExpCogniteClient
from crontab import CronSlices
from pydantic import BaseModel, root_validator, validator

# TenantConfig parameters:
KEY_CDF_PROJECT = "cdf_project"
KEY_DEPLOYMENT_KEY = "deployment_key"
KEY_RUNTIME_KEY = "runtime_key"
KEY_CDF_BASE_URL = "cdf_base_url"
CLIENT_NAME_FUNC_ACTION = "function-action-validator"


class TenantConfig(BaseModel):
    cdf_project: Optional[str]
    deployment_key: str
    runtime_key: str
    cdf_base_url: Optional[str]

    @validator(KEY_CDF_PROJECT, pre=True)
    def valid_project(cls, value):
        if isinstance(value, str) and value == "":
            raise ValueError("CDF project should not be an empty string.")
        return value

    @validator(KEY_DEPLOYMENT_KEY, pre=True)
    def valid_deployment_key(cls, value):
        if value is None:
            raise ValueError("Missing Cognite Functions deployment API-key.'")
        elif value == "":
            raise ValueError("Deployment API-key for Cognite Functions should not be empty.")
        return value

    @validator(KEY_RUNTIME_KEY, pre=True)
    def valid_runtime_key(cls, value):
        if value is None:
            raise ValueError("Missing Cognite Functions runtime API-key.'")
        elif value == "":
            raise ValueError("Runtime API-key for Cognite Functions should not be empty.")
        return value

    @validator(KEY_CDF_BASE_URL, pre=True)
    def valid_cdf_base_url_key(cls, value):
        if value is None:
            return "https://api.cognitedata.com"
        if isinstance(value, str) and value.strip() == "":
            raise ValueError("CDF base url should not be an empty string.")
        return value

    @root_validator()
    def check_credentials(cls, values):
        project = values.get(KEY_CDF_PROJECT)
        if project is None:
            return values

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            deployment_client = CogniteClient(
                api_key=values.get(KEY_DEPLOYMENT_KEY),
                base_url=values.get(KEY_CDF_BASE_URL),
                client_name=CLIENT_NAME_FUNC_ACTION,
            )
        if not deployment_client.login.status().logged_in:
            raise ValueError("Can't login with deployment credentials")

        inferred_project = deployment_client.login.status().project
        if inferred_project != project:
            raise ValueError(
                f"Inferred project, {inferred_project}, from the provided deployment credentials "
                f"does not match the project defined: {project}"
            )
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            runtime_client = CogniteClient(
                api_key=values.get(KEY_RUNTIME_KEY),
                base_url=values.get(KEY_CDF_BASE_URL),
                client_name=CLIENT_NAME_FUNC_ACTION,
            )
        if not runtime_client.login.status().logged_in:
            raise ValueError("Can't login with runtime credentials")

        if runtime_client.login.status().project != project:
            raise ValueError(f"Provided runtime credentials doesn't match the project defined: {project}")
        return values


def create_experimental_cognite_client(config: TenantConfig) -> ExpCogniteClient:
    return ExpCogniteClient(
        api_key=config.deployment_key,
        project=config.cdf_project,
        base_url=config.cdf_base_url,
        client_name="function-action",
    )


class ScheduleConfig(BaseModel):
    cron: str
    name: str
    data: Dict

    @validator("cron")
    def valid_cron(cls, value):
        value = value.strip()
        if not CronSlices.is_valid(value):
            raise ValueError(f"Invalid cron expression: '{value}'")
        return value


def decode_and_parse(value) -> Optional[Dict]:
    if value is None:
        return None
    decoded = base64.b64decode(value.encode())
    return json.loads(decoded)


class FunctionConfig(BaseModel):
    external_id: str
    folder_path: Path
    common_folder_path: Optional[Path]
    file: str
    schedule_file: Optional[str]
    data_set_external_id: Optional[str]
    secret: Optional[str]
    tenant: TenantConfig
    overwrite: bool
    remove_only: bool = False
    deploy_wait_time_sec: int = 1200  # 20 minutes
    cpu: Optional[float]
    memory: Optional[float]

    @validator("file")
    def valid_file(cls, value):
        if not value.endswith(".py"):
            raise ValueError(f"Invalid file name, must end with '.py', but got '{value}'")
        return value

    @validator("schedule_file")
    def valid_schedule_file(cls, value):
        allowed_file_suffixes = [".yml", ".yaml"]
        if value is not None and Path(value).suffix not in allowed_file_suffixes:
            raise ValueError(f"Invalid file suffix for '{value}', expected {' or '.join(allowed_file_suffixes)}")
        return value

    @validator("secret")
    def valid_secret(cls, value):
        try:
            decode_and_parse(value)
        except Exception as e:
            raise ValueError("Invalid secret, must be a valid base64 encoded json") from e
        return value

    @root_validator()
    def check_folder_paths(cls, values):
        def is_dir_validator(value):
            if not isinstance(value, Path):
                value = Path(value)
            if not value.is_dir():
                raise ValueError(f"Invalid folder value: '{value}', not a directory!")
            return value

        values["folder_path"] = is_dir_validator(values["folder_path"])
        common_folder_path = values["common_folder_path"]
        if common_folder_path is not None:
            values["common_folder_path"] = is_dir_validator(common_folder_path)
        else:
            # Try default directory 'common/':
            with contextlib.suppress(ValueError):
                values["common_folder_path"] = is_dir_validator("common")
        return values

    @root_validator()
    def check_schedules(cls, values):
        file = values.get("schedule_file")
        folder = values.get("folder_path")
        if file is not None and folder is not None:
            path = Path(folder) / Path(file)
            if not path.exists() or not path.is_file():
                raise ValueError(f"Schedules file doesn't exist at path: {path.absolute()}")
        return values

    @property
    def schedules(self) -> List[ScheduleConfig]:
        if self.schedule_file is not None:
            path = Path(self.folder_path) / Path(self.schedule_file)
            with path.open(mode="r") as f:
                collection: List[Dict] = yaml.safe_load(f.read())
            return [
                ScheduleConfig(
                    cron=col.get("cron"),
                    name=self.external_id + ":" + col.get("name", f"undefined-{i}"),
                    data=col.get("data", {}),
                )
                for i, col in enumerate(collection)
            ]
        return []

    @property
    def unpacked_secrets(self) -> Optional[Dict]:
        return decode_and_parse(self.secret)
