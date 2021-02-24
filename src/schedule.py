import logging

from cognite.experimental import CogniteClient
from cognite.experimental.data_classes import Function

from config import FunctionConfig

logger = logging.getLogger(__name__)


def delete_all_schedules_attached(client: CogniteClient, function: Function):
    """
    We want to delete ALL existing schedules since we don't keep state anywhere and we want to wipe:
      1. Those we are going to recreate
      2. Those removed permanently
    """
    # Note for myself in the future:
    # function.function.list_schedules() doesn't return old/orphan schedules at the time that was written
    schedule_ids = [
        schedule.id
        for schedule in client.functions.schedules.list(limit=None)
        if schedule.function_external_id == function.external_id
    ]
    if schedule_ids:
        for sid in schedule_ids:  # TODO: Experimental SDK does not support "delete multiple"
            client.functions.schedules.delete(sid)
        logger.info(f"Deleted all ({len(schedule_ids)}) existing schedule(s)!")


def deploy_schedule(client: CogniteClient, function: Function, config: FunctionConfig):
    delete_all_schedules_attached(client, function)

    if not config.schedules:
        logger.info("Skipped step of attaching schedules!")
        return

    for schedule in config.schedules:
        client.functions.schedules.create(
            function_external_id=function.external_id,
            cron_expression=schedule.cron,
            name=schedule.name,
            data=schedule.data,
        )
        logger.info(f"Successfully deployed schedule {schedule.name} with cron expression {schedule.cron}.")
