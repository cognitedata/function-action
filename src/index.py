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
FUNCTION_FOLDER = os.getenv("INPUT_FUNCTION_FOLDER")
FUNCTION_PATH = os.getenv("INPUT_FUNCTION_PATH")

# Input used for deploying using a configuration file
FUNCTION_NAME = os.getenv("INPUT_FUNCTION_NAME", "")
CONFIG_FILE_PATH = os.getenv("INPUT_CONFIG_FILE_PATH")

GITHUB_EVENT_NAME = os.environ["GITHUB_EVENT_NAME"]
GITHUB_REF = os.environ["GITHUB_REF"]


print(f"Handling event {GITHUB_EVENT_NAME} on {GITHUB_REF}")


def handle_config_file():
    if not FUNCTION_NAME:
        raise MissingInput("Missing function_name input")
    config_file_path = Path(CONFIG_FILE_PATH)
    if not config_file_path.exists():
        raise IOError(f"Could not find {config_file_path}")

    config = get_config(config_file_path, FUNCTION_NAME)

    if FUNCTION_NAME not in config.functions:
        raise ValueError(f"Could not find function named {FUNCTION_NAME} in configuration file")

    function_config = config.functions[FUNCTION_NAME]

    for tenant in function_config.tenants:
        client = CogniteClient(
            api_key=os.getenv(tenant.deployment_key_name),
            project=tenant.cdf_project,
            base_url=tenant.cdf_base_url,
            client_name="deploy-function-action",
        )
        function = call_deploy(
            client,
            config.function_folder,
            function_config.path,
            os.getenv(tenant.function_key_name),
        )

        if function:
            deploy_schedule(
                client,
                function,
                function_config.schedule.name,
                function_config.schedule.cron,
            )


def handle_single_function():
    if not (
        CDF_PROJECT and CDF_DEPLOYMENT_CREDENTIALS and FUNCTION_FOLDER and FUNCTION_PATH and CDF_FUNCTION_CREDENTIALS
    ):
        raise MissingInput(
            "Missing one of inputs cdf_project, cdf_deployment_credentials, "
            "function_folder, function_path, function_credentials"
        )

    client = CogniteClient(
        api_key=CDF_DEPLOYMENT_CREDENTIALS,
        project=CDF_PROJECT,
        base_url=CDF_BASE_URL,
        client_name="deploy-function-action",
    )
    call_deploy(client, FUNCTION_FOLDER, FUNCTION_PATH, CDF_FUNCTION_CREDENTIALS)


def call_deploy(client: CogniteClient, function_folder, function_path, api_key) -> Function:
    user = client.login.status()
    assert user.logged_in
    if GITHUB_EVENT_NAME == "push":
        return deploy_function(client, function_folder, function_path, api_key)
    elif GITHUB_EVENT_NAME == "pull_request":
        return deploy_function(client, function_folder, function_path, api_key, is_pr=True)


if CONFIG_FILE_PATH:
    handle_config_file()
else:
    handle_single_function()
