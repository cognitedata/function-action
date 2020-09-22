import os
from pathlib import Path

from cognite.experimental import CogniteClient
from cognite.experimental.data_classes import Function

from config import get_config
from function import deploy_function
from schedule import deploy_schedule


class MissingInput(Exception):
    pass


class MissingConfig(Exception):
    pass


# Input used for deploying function where all metadata is added directly in the Github workflow
CDF_PROJECT = os.getenv("INPUT_CDF_PROJECT")
CDF_DEPLOYMENT_CREDENTIALS = os.getenv("INPUT_CDF_DEPLOYMENT_CREDENTIALS")
CDF_FUNCTION_CREDENTIALS = os.getenv("INPUT_CDF_FUNCTION_CREDENTIALS")
CDF_BASE_URL = os.getenv("INPUT_CDF_BASE_URL")
FUNCTION_FILE = os.getenv("INPUT_FUNCTION_FILE")
FUNCTION_FOLDER = os.getenv("INPUT_FUNCTION_FOLDER")

# Input used for deploying using a configuration file
FUNCTION_NAME = os.getenv("INPUT_FUNCTION_NAME", "")
CONFIG_FILE_PATH = os.getenv("INPUT_CONFIG_FILE_PATH")

GITHUB_EVENT_NAME = os.environ["GITHUB_EVENT_NAME"]
GITHUB_REF = os.environ["GITHUB_REF"]
IS_DELETE = os.getenv("DELETE_PR_FUNCTION")
IS_PR = GITHUB_EVENT_NAME == "pull_request"
IS_PUSH = GITHUB_EVENT_NAME == "push"


print(f"Handling event {GITHUB_EVENT_NAME} on {GITHUB_REF}")


def handle_config_file():
    if not FUNCTION_NAME:
        raise MissingInput("Missing function_name input")
    config_file_path = Path(CONFIG_FILE_PATH)
    if not config_file_path.exists():
        raise IOError(f"Could not find {config_file_path}")

    config = get_config(config_file_path, FUNCTION_NAME)

    for tenant in config.tenants:
        client = CogniteClient(
            api_key=os.getenv(tenant.deployment_key_name),
            project=tenant.cdf_project,
            base_url=tenant.cdf_base_url,
            client_name="deploy-function-action",
        )
        function = call_deploy(
            client,
            config.folder_path,
            config.file,
            os.getenv(tenant.function_key_name),
        )

        if not IS_PR and function and config.schedules:
            for schedule in config.schedules:
                deploy_schedule(
                    client,
                    function,
                    schedule.name,
                    schedule.cron,
                )


def handle_single_function():
    if not (
        CDF_PROJECT and CDF_DEPLOYMENT_CREDENTIALS and FUNCTION_FOLDER and FUNCTION_FILE and CDF_FUNCTION_CREDENTIALS
    ):
        raise MissingInput(
            "Missing one of inputs cdf_project, cdf_deployment_credentials, "
            "function_folder, function_file, function_credentials"
        )

    client = CogniteClient(
        api_key=CDF_DEPLOYMENT_CREDENTIALS,
        project=CDF_PROJECT,
        base_url=CDF_BASE_URL,
        client_name="deploy-function-action",
    )
    call_deploy(client, FUNCTION_FOLDER, FUNCTION_FILE, CDF_FUNCTION_CREDENTIALS)


def call_deploy(client: CogniteClient, function_folder, function_path, api_key) -> Function:
    user = client.login.status()
    assert user.logged_in
    if IS_PUSH:
        return deploy_function(client, function_folder, function_path, api_key, is_delete=IS_DELETE)
    elif IS_PR:
        return deploy_function(client, function_folder, function_path, api_key, is_pr=True, is_delete=IS_DELETE)


if CONFIG_FILE_PATH:
    handle_config_file()
else:
    handle_single_function()
