import os
from pathlib import Path
from typing import Dict, List, Optional

import yaml
from cognite.client import CogniteClient
from crontab import CronSlices
from pydantic import BaseModel, root_validator, validator


class InvalidCronException(Exception):
    pass


class TenantConfig(BaseModel):
    cdf_project: Optional[str]
    deployment_key: str
    runtime_key: str
    cdf_base_url: str

    @root_validator()
    def check_credentials(cls, values):
        project = values.get("cdf_project")
        if project is not None:
            deployment_client = CogniteClient(
                api_key=values.get("deployment_key"),
                base_url=values.get("cdf_base_url"),
                client_name="function-action-validator",
            )
            if not deployment_client.login.status().logged_in:
                raise ValueError("Can't login with deployment credentials")

            if deployment_client.login.status().project != project:
                raise ValueError(f"Provided deployment credentials doesn't match the project defined: {project}")

            runtime_client = CogniteClient(
                api_key=values.get("runtime_key"),
                base_url=values.get("cdf_base_url"),
                client_name="function-action-validator",
            )
            if not runtime_client.login.status().logged_in:
                raise ValueError("Can't login with runtime credentials")

            if runtime_client.login.status().project != project:
                raise ValueError(f"Provided runtime credentials doesn't match the project defined: {project}")

        return values


class ScheduleConfig(BaseModel):
    cron: str
    name: str
    data: Dict

    @validator("cron")
    def valid_cron(cls, value):
        if not CronSlices.is_valid(value):
            raise InvalidCronException(f"Invalid cron expression: '{value}'")

        return value.strip()


class FunctionConfig(BaseModel):
    external_id: str
    folder_path: str
    file: str
    schedule_file: Optional[str]
    tenant: TenantConfig
    remove_only: bool

    @validator("file")
    def valid_file(cls, value):
        if not value.endswith(".py"):
            raise ValueError(f"Invalid file name, must end with '.py', but got '{value}'")
        return value

    @validator("schedule_file")
    def valid_schedule_file(cls, value):
        if value is not None and not (value.endswith(".yml") or value.endswith(".yaml")):
            raise ValueError(f"Invalid file name, must end with '.yml' or '.yaml', but got '{value}'")
        return value

    @root_validator()
    def check_schedules(cls, values):
        file = values.get("schedule_file", None)
        folder = values.get("folder_path")
        if file is not None and folder is not None:
            path = Path(folder + "/" + file)
            if not (path.exists() and path.is_file()):
                raise ValueError(f"Schedules file doesn't exist at path: {path.absolute()}")
        return values

    @property
    def schedules(self) -> List[ScheduleConfig]:
        if self.schedule_file is not None:
            path = Path(self.folder_path + "/" + self.schedule_file)
            collection: List[Dict] = yaml.safe_load(path.open(mode="r").read())
            return [
                ScheduleConfig(
                    cron=c.get("cron"), name=self.external_id + ":" + c.get("name", "undefined"), data=c.get("data", {})
                )
                for c in collection
            ]

        return []
