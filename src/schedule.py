import logging

from cognite.experimental import CogniteClient
from cognite.experimental.data_classes import Function

from config import FunctionConfig

logger = logging.getLogger(__name__)


def delete_all_schedules_for_ext_id(client: CogniteClient, function_external_id: str):
    """
    We want to delete ALL existing schedules since we don't keep state anywhere and we want to wipe:
      1. Those we are going to recreate
      2. Those removed permanently
    """
    all_schedules = client.functions.schedules.list(function_external_id=function_external_id, limit=None)
    if all_schedules:
        for s in all_schedules:  # TODO: Experimental SDK does not support "delete multiple"
            client.functions.schedules.delete(s.id)
        logger.info(f"Deleted all ({len(all_schedules)}) existing schedule(s)!")
    else:
        logger.info("No existing schedule(s) to delete!")


def deploy_schedule(client: CogniteClient, function: Function, config: FunctionConfig):
    if not config.schedules:
        logger.info("No schedules to attach!")
        return

    logger.info(f"Attaching {len(config.schedules)} schedule(s) to {function.external_id}")
    for schedule in config.schedules:
        client.functions.schedules.create(
            function_external_id=function.external_id,
            cron_expression=schedule.cron,
            name=schedule.name,
            data=schedule.data,
        )
        logger.info(f"- Schedule '{schedule.name}' with cron: '{schedule.cron}' attached successfully!")
