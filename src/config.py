import base64
import contextlib
import json
import logging
import warnings
from pathlib import Path
from typing import Dict, List, Optional

import yaml
from cognite.client import CogniteClient
from cognite.experimental import CogniteClient as ExpCogniteClient
from crontab import CronSlices
from pydantic import BaseModel, root_validator, validator, constr


logger = logging.getLogger(__name__)

# Pydantic fields:
non_empty_str = constr(min_length=1, strip_whitespace=True)

DEPLOY_WAIT_TIME_SEC = 1200  # 20 minutes


class TenantConfig(BaseModel):
    cdf_project: non_empty_str
    deployment_key: non_empty_str
    runtime_key: non_empty_str
    cdf_base_url: non_empty_str

    @root_validator()
    def check_credentials(cls, values):
        project = values["cdf_project"]
        kwargs = {
            "base_url": values["cdf_base_url"],
            "client_name": "function-action-validator",
            "disable_pypi_version_check": True,
        }
        for env in ["deployment", "runtime"]:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=UserWarning)
                client = CogniteClient(api_key=values[f"{env}_key"], **kwargs)
            if not client.login.status().logged_in:
                raise ValueError(f"Can't login with {env} credentials")

            inferred_project = client.login.status().project
            if inferred_project != project:
                raise ValueError(
                    f"Inferred project, {inferred_project}, from the provided {env} credentials "
                    f"does not match the given project: {project}"
                )
        return values


def create_experimental_cognite_client(config: TenantConfig) -> ExpCogniteClient:
    return ExpCogniteClient(
        api_key=config.deployment_key,
        project=config.cdf_project,
        base_url=config.cdf_base_url,
        client_name="function-action",
        disable_pypi_version_check=True,
    )


class ScheduleConfig(BaseModel):
    name: non_empty_str
    cron: non_empty_str
    data: Optional[Dict]

    @validator("cron")
    def valid_cron(cls, value):
        if not CronSlices.is_valid(value):
            raise ValueError(f"Invalid cron expression: '{value}'")
        return value


def decode_and_parse(value) -> Optional[Dict]:
    if value is None:
        return None
    decoded = base64.b64decode(value.encode())
    return json.loads(decoded)


def verify_path_is_directory(path):
    if not path.is_dir():
        raise ValueError(f"Invalid folder path: '{path}', not a directory!")
    return path


class FunctionConfig(BaseModel):
    external_id: str
    folder_path: Path
    common_folder_path: Path = None
    file: constr(min_length=1, strip_whitespace=True, regex=r"^[\w\- ]+\.py$")
    schedule_file: constr(min_length=1, strip_whitespace=True, regex=r"^[\w\- ]+\.ya?ml$") = None
    data_set_external_id: str = None
    secret: str = None
    tenant: TenantConfig
    remove_only: bool = False
    cpu: float = None
    memory: float = None
    owner: constr(min_length=1, max_length=128, strip_whitespace=True) = None

    @validator("secret")
    def valid_secret(cls, value):
        if value is None:
            return value
        try:
            decode_and_parse(value)
        except Exception as e:
            raise ValueError("Invalid secret, must be a valid base64 encoded json") from e
        return value

    @root_validator()
    def check_folder_paths(cls, values):
        verify_path_is_directory(values["folder_path"])

        common_folder_path = values["common_folder_path"]
        if common_folder_path is not None:
            verify_path_is_directory(common_folder_path)
        else:
            # Try default directory 'common/':
            with contextlib.suppress(ValueError):
                values["common_folder_path"] = verify_path_is_directory(Path("common"))
        return values

    @root_validator(skip_on_failure=True)
    def check_schedules(cls, values):
        schedule_file = values["schedule_file"]
        if schedule_file is None:
            return values
        path = values["folder_path"] / schedule_file
        if not path.is_file():
            values["schedule_file"] = None
            logger.warning(f"Ignoring given schedule file '{schedule_file}', path does not exist: {path.absolute()}")
        return values

    @property
    def schedules(self) -> List[ScheduleConfig]:
        if self.schedule_file is None:
            return []
        path = self.folder_path / self.schedule_file
        with path.open() as f:
            all_schedules = yaml.safe_load(f)
        return [
            ScheduleConfig(
                cron=schedule.get("cron"),  # If missing, we let Pydantic handle it
                name=self.external_id + ":" + schedule.get("name", f"undefined-{i}"),
                data=schedule.get("data"),
            )
            for i, schedule in enumerate(all_schedules)
        ]

    @property
    def unpacked_secrets(self) -> Optional[Dict]:
        return decode_and_parse(self.secret)

    def get_memory_and_cpu(self):
        kw = {}
        if self.memory is not None:
            kw["memory"] = self.memory
        if self.cpu is not None:
            kw["cpu"] = self.cpu
        return kw
