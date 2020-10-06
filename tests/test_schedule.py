from unittest.mock import MagicMock, call, patch

from cognite.experimental.data_classes.functions import FunctionSchedule

from schedule import deploy_schedule


def test_deploy_schedule(cognite_experimental_client_mock, valid_config):
    schedules = [
        FunctionSchedule(id=1, name=valid_config.schedules[0].name),
        FunctionSchedule(id=2, name="random schedule"),
    ]

    function_mock = MagicMock()
    function_mock.list_schedules.return_value = schedules
    function_mock.external_id = valid_config.external_id

    deploy_schedule(cognite_experimental_client_mock, function_mock, valid_config)

    assert cognite_experimental_client_mock.functions.schedules.delete.call_args_list == [call(1), call(2)]
    assert cognite_experimental_client_mock.functions.schedules.create.call_args_list == [
        call(
            function_external_id=valid_config.external_id,
            cron_expression=valid_config.schedules[0].cron,
            name=valid_config.schedules[0].name,
        )
    ]
