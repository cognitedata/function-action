from unittest.mock import MagicMock, call

from schedule import deploy_schedules


def test_deploy_schedules(cognite_experimental_client_mock, valid_config):
    function_mock = MagicMock()
    function_mock.external_id = valid_config.external_id

    deploy_schedules(cognite_experimental_client_mock, function_mock, valid_config.schedules)

    assert cognite_experimental_client_mock.functions.schedules.create.call_args_list == [
        call(
            function_external_id=valid_config.external_id,
            cron_expression=valid_config.schedules[0].cron,
            name=valid_config.schedules[0].name,
            data=valid_config.schedules[0].data,
        )
    ]
