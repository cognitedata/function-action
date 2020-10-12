import logging
import os
from typing import Dict, Optional

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
    fn = deploy_function(client, config)

    if fn is not None:
        deploy_schedule(client, fn, config)

    return fn


class GitHubLogHandler(logging.StreamHandler):
    def __init__(self, stream=None):
        super(GitHubLogHandler, self).__init__(stream=stream)

    # https://docs.github.com/en/free-pro-team@latest/actions/reference/workflow-commands-for-github-actions#setting-a-debug-message
    def format(self, record):
        message = super(GitHubLogHandler, self).format(record)
        level_map: Dict = {
            logging.CRITICAL: "error",
            logging.ERROR: "error",
            logging.WARNING: "warning",
            logging.INFO: "warning",
            logging.DEBUG: "debug",
            logging.NOTSET: "warning",
        }
        return (
            f"::{level_map.get(record.levelno)} file={record.filename},line={record.levelno}::{record.name}: {message}"
        )


if __name__ == "__main__":
    handler = GitHubLogHandler()
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)

    # Input used for deploying using a configuration file
    function = main(
        FunctionConfig(
            external_id=os.getenv("INPUT_FUNCTION_NAME", ""),
            folder_path=os.getenv("INPUT_FUNCTION_FOLDER", ""),
            file=os.getenv("INPUT_FUNCTION_FILE", "handler.py"),
            tenant=TenantConfig(
                cdf_project=os.getenv("INPUT_CDF_PROJECT"),
                deployment_key=os.getenv("INPUT_CDF_DEPLOYMENT_CREDENTIALS"),
                runtime_key=os.getenv("INPUT_CDF_RUNTIME_CREDENTIALS"),
                cdf_base_url=os.getenv("INPUT_CDF_BASE_URL"),
            ),
            schedule_file=os.getenv("INPUT_SCHEDULE_FILE") or None,
            remove_only=os.getenv("INPUT_REMOVE_ONLY", False),
        )
    )
    if function is not None:
        # Return output parameter:
        print(f"::set-output name=function_external_id::{function.external_id}")
