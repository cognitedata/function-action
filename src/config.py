import os
from typing import List, Optional

from cognite.client import CogniteClient
from crontab import CronSlices
from pydantic import BaseModel, root_validator, validator


class InvalidCronException(Exception):
    pass


class TenantConfig(BaseModel):
    cdf_project: Optional[str]
    deployment_key_name: str
    runtime_key_name: str
    cdf_base_url: str

    @validator("deployment_key_name", "runtime_key_name")
    def key_exists(cls, key_name):
        if not os.getenv(key_name):
            raise ValueError(f"Environment variable named {key_name} not set")
        return key_name

    @root_validator()
    def check_credentials(cls, values):
        project = values.get("cdf_project")
        if project is not None:
            deployment_client = CogniteClient(
                api_key=os.getenv(values.get("deployment_key_name")),
                base_url=values.get("cdf_base_url"),
                client_name="function-action-validator",
            )
            if not deployment_client.login.status().logged_in:
                raise ValueError("Can't login with deployment credentials")

            if deployment_client.login.status().project != project:
                raise ValueError(f"Provided deployment credentials doesn't match the project defined: {project}")

            runtime_client = CogniteClient(
                api_key=os.getenv(values.get("runtime_key_name")),
                base_url=values.get("cdf_base_url"),
                client_name="function-action-validator",
            )
            if not runtime_client.login.status().logged_in:
                raise ValueError("Can't login with runtime credentials")

            if runtime_client.login.status().project != project:
                raise ValueError(f"Provided runtime credentials doesn't match the project defined: {project}")

        return values

    @property
    def runtime_key(self):
        return os.getenv(self.runtime_key_name)

    @property
    def deployment_key(self):
        return os.getenv(self.deployment_key_name)


class ScheduleConfig(BaseModel):
    cron: str
    name: str

    @validator("cron")
    def valid_cron(cls, value):
        if not CronSlices.is_valid(value):
            raise InvalidCronException(f"Invalid cron expression: '{value}'")

        return value.strip()


class FunctionConfig(BaseModel):
    external_id: str
    folder_path: str
    file: str
    schedules: List[ScheduleConfig]
    tenant: TenantConfig
    remove_only: bool

    @validator("file")
    def valid_file(cls, value):
        if not value.endswith(".py"):
            raise ValueError(f"Invalid file name, must end with '.py', but got '{value}'")
        return value
