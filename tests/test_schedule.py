from unittest.mock import MagicMock, call, patch

import pytest
from cognite.experimental.data_classes.functions import FunctionSchedule

from src.schedule import deploy_schedule


@pytest.mark.unit
@patch("src.schedule.CogniteClient")
def test_deploy_schedule(client_mock):
    schedules = [FunctionSchedule(id=1, name="s1"), FunctionSchedule(id=2, name="s2")]

    function_mock = MagicMock()
    function_mock.list_schedules.return_value = schedules
    function_mock.external_id = "id3"

    deploy_schedule(client_mock, function_mock, "s1", "my cron")

    assert client_mock.functions.schedules.delete.call_args_list == [call(1)]
    assert client_mock.functions.schedules.create.call_args_list == [
        call(function_external_id="id3", cron_expression="my cron", name="s1")
    ]
