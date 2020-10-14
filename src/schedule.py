import logging

from cognite.experimental import CogniteClient
from cognite.experimental.data_classes import Function

from config import FunctionConfig

logger = logging.getLogger(__name__)


def deploy_schedule(client: CogniteClient, function: Function, config: FunctionConfig):
    for schedule in function.list_schedules():
        # We want to delete ALL existing schedules since we don't keep state anywhere and we want to wipe:
        # 1. Those we are going to recreate
        # 2. Those removed permanently
        client.functions.schedules.delete(schedule.id)

    for schedule in config.schedules:
        client.functions.schedules.create(
            function_external_id=function.external_id,
            cron_expression=schedule.cron,
            name=schedule.name,
            data=schedule.data,
        )
        logger.info(f"Successfully deployed schedule {schedule.name} with cron expression {schedule.cron}.")
