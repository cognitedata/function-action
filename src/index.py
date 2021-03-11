import logging
import os

from config import FunctionConfig, TenantConfig, create_experimental_cognite_client
from function import try_delete, upload_and_create
from github_log_handler import GitHubLogHandler
from schedule import deploy_schedule

# Configure logging:
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
root_logger.addHandler(GitHubLogHandler())

logger = logging.getLogger(__name__)


def main(config: FunctionConfig) -> None:
    client = create_experimental_cognite_client(config.tenant)

    # Delete old function and file:
    try_delete(client, config.external_id)
    if config.remove_only:
        return

    # Zip files, upload to Files and create CogFunc:
    function = upload_and_create(client, config)
    logger.info(f"Successfully created and deployed function {config.external_id} with id {function.id}")
    deploy_schedule(client, function, config)

    # Return output parameter:
    print(f"::set-output name=function_external_id::{function.external_id}")


def setup_config() -> FunctionConfig:
    for k, v in os.environ.items():
        if k.startswith("INPUT"):
            logger.info(k, v)
    # Note: The "or None" is there so that - if the empty string is passed - we pass None instead.
    return FunctionConfig(
        external_id=os.getenv("INPUT_FUNCTION_NAME", ""),
        folder_path=os.getenv("INPUT_FUNCTION_FOLDER", ""),
        common_folder_path=os.getenv("INPUT_COMMON_FOLDER"),
        file=os.getenv("INPUT_FUNCTION_FILE", "handler.py"),
        data_set_external_id=os.getenv("INPUT_DATA_SET_EXTERNAL_ID") or None,
        tenant=TenantConfig(
            cdf_project=os.getenv("INPUT_CDF_PROJECT") or None,
            deployment_key=os.getenv("INPUT_CDF_DEPLOYMENT_CREDENTIALS"),
            runtime_key=os.getenv("INPUT_CDF_RUNTIME_CREDENTIALS"),
            cdf_base_url=os.getenv("INPUT_CDF_BASE_URL") or None,
        ),
        secret=os.getenv("INPUT_FUNCTION_SECRETS") or None,
        schedule_file=os.getenv("INPUT_SCHEDULE_FILE") or None,
        remove_only=os.getenv("INPUT_REMOVE_ONLY"),
        overwrite=os.getenv("INPUT_OVERWRITE"),
        cpu=os.getenv("INPUT_CPU") or None,
        memory=os.getenv("INPUT_MEMORY") or None,
        owner=os.getenv("INPUT_OWNER") or None,
    )


if __name__ == "__main__":
    # Function Action, assemble!!
    config = setup_config()
    main(config)
