from cognite.experimental import CogniteClient
from cognite.experimental.data_classes import Function


def deploy_schedule(client: CogniteClient, function: Function, schedule_name: str, cron: str):
    schedules = function.list_schedules()

    for schedule in schedules:
        if schedule.name == schedule_name:
            client.functions.schedules.delete(schedule.id)
            break

    client.functions.schedules.create(
        function_external_id=function.external_id,
        cron_expression=cron,
        name=schedule_name,
    )

    print(f"Successfully deployed schedule {schedule_name} with cron expression {cron}.")
