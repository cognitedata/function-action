import logging
import os
from typing import Dict, List, Optional

import yaml
from cognite.experimental import CogniteClient
from cognite.experimental.data_classes import Function

from config import FunctionConfig, TenantConfig
from function import deploy_function
from schedule import deploy_schedule


def main(config: FunctionConfig) -> Optional[Function]:
    client = CogniteClient(
        api_key=config.tenant.deployment_key,
        project=config.tenant.cdf_project,
        base_url=config.tenant.cdf_base_url,
        client_name="function-action",
    )
    f = deploy_function(client, config)

    if f is not None:
        deploy_schedule(client, f, config)

    return f


class GitHubLogHandler(logging.StreamHandler):
    def __init__(self, stream=None):
        super(GitHubLogHandler, self).__init__(stream=stream)

    # https://docs.github.com/en/free-pro-team@latest/actions/reference/workflow-commands-for-github-actions#setting-a-debug-message
    def format(self, record):
        message = super(LogHandler, self).format(record)
        level_map: Dict = {
            logging.CRITICAL: "warning",
            logging.ERROR: "error",
            logging.WARNING: "warning",
            logging.INFO: "debug",
            logging.DEBUG: "debug",
            logging.NOTSET: "warning",
        }
        return (
            f"::{level_map.get(record.levelno)} file={record.filename},line={record.levelno}::{record.name}: {message}"
        )


if __name__ == "__main__":
    handler = GitHubLogHandler()
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)

    # Input used for deploying using a configuration file

    GITHUB_EVENT_NAME = os.getenv("GITHUB_EVENT_NAME", "undefined")
    GITHUB_REF = os.getenv("GITHUB_REF", "undefined")

    schedules: List[str] = yaml.safe_load(os.getenv("INPUT_SCHEDULES", "[]"))
    ext_id = os.getenv("INPUT_FUNCTION_NAME", "")
    schedule_file = os.getenv("INPUT_SCHEDULE_FILE", None)
    cdf_project = os.getenv("INPUT_CDF_PROJECT", None)
    function = main(
        FunctionConfig(
            external_id=ext_id,
            folder_path=os.getenv("INPUT_FUNCTION_FOLDER", ""),
            file=os.getenv("INPUT_FUNCTION_FILE", "handler.py"),
            tenant=TenantConfig(
                cdf_project=cdf_project if cdf_project and cdf_project != "" else None,
                deployment_key=os.getenv("INPUT_CDF_DEPLOYMENT_CREDENTIALS", ""),
                runtime_key=os.getenv("INPUT_CDF_RUNTIME_CREDENTIALS", ""),
                cdf_base_url=os.getenv("INPUT_CDF_BASE_URL", ""),
            ),
            schedule_file=schedule_file if schedule_file and schedule_file != "" else None,
            remove_only=os.getenv("INPUT_REMOVE_ONLY"),
        )
    )

    if function is not None:
        # return output parameter
        print(f"::set-output name=function_external_id::{function.external_id}")
