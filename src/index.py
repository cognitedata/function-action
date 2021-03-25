import logging
import os

import yaml

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
    if config.remove_only:
        # Delete old function and file:
        try_delete(client, config.external_id)
        return

    # Zip files, upload to Files and create CogFunc:
    function = upload_and_create(client, config)
    logger.info(f"Successfully created and deployed function {config.external_id} with id {function.id}")
    deploy_schedule(client, function, config)

    # Return output parameter (GitHub magic syntax):
    print(f"::set-output name=function_external_id::{function.external_id}")


def setup_config() -> FunctionConfig:
    # Use 'action.yaml' as the single source of thruth for param names:
    with open("action.yaml") as f:
        inputs = set(yaml.safe_load(f)["inputs"])

    tenant_params = [inp for inp in inputs if inp.startswith("cdf")]
    function_params = inputs.difference(tenant_params)

    return FunctionConfig(
        tenant=TenantConfig(**{p: os.getenv(f"INPUT_{p.upper()}") for p in tenant_params}),
        **{p: os.getenv(f"INPUT_{p.upper()}") for p in function_params},
    )


if __name__ == "__main__":
    # Function Action, assemble!!
    config = setup_config()
    main(config)
