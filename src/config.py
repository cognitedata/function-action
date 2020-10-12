from pathlib import Path
from typing import Dict, List, Optional

import yaml
from cognite.client import CogniteClient
from crontab import CronSlices
from pydantic import BaseModel, root_validator, validator


class TenantConfig(BaseModel):
    cdf_project: Optional[str]
    deployment_key: str
    runtime_key: str
    cdf_base_url: str

    @validator("cdf_project", pre=True)
    def valid_project(cls, value):
        if value is None:
            raise ValueError("Missing CDF project.")
        elif value == "":
            raise ValueError("CDF project should not be empty.")
        return value

    @validator("deployment_key", pre=True)
    def valid_deployment_key(cls, value):
        if value is None:
            raise ValueError("Missing deployment key.'")
        elif value == "":
            raise ValueError("Deployment key should not be empty.")
        return value

    @validator("runtime_key", pre=True)
    def valid_runtime_key(cls, value):
        if value is None:
            raise ValueError("Missing runtime key.'")
        elif value == "":
            raise ValueError("Runtime key should not be empty.")
        return value

    @root_validator()
    def check_credentials(cls, values):
        project = values["cdf_project"]
        if project is not None:
            deployment_client = CogniteClient(
                api_key=values.get("deployment_key"),
                base_url=values.get("cdf_base_url"),
                client_name="function-action-validator",
            )
            if not deployment_client.login.status().logged_in:
                raise ValueError("Can't login with deployment credentials")

            inferred_project = deployment_client.login.status().project
            if inferred_project != project:
                raise ValueError(
                    f"Inferred project, {inferred_project}, from the provided deployment credentials "
                    f"does not match the project defined: {project}"
                )

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
        value = value.strip()
        if not CronSlices.is_valid(value):
            raise ValueError(f"Invalid cron expression: '{value}'")
        return value


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
        file = values.get("schedule_file")
        folder = values.get("folder_path")
        if file is not None and folder is not None:
            path = Path(folder) / Path(file)
            if not (path.exists() and path.is_file()):
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
                    name=self.external_id + ":" + col.get("name", "undefined"),
                    data=col.get("data", {}),
                )
                for col in collection
            ]
        return []
