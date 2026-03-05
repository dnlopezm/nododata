"""Tests for CLI process commands."""

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from nodo_etl.cli.main import cli
from nodo_etl.db.repositories import NotFoundError


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def mock_conn():
    mock = MagicMock()
    mock.schema_name = "nodo_etl"
    mock.dialect = MagicMock()
    mock.dialect.dialect_type = MagicMock()
    mock.dialect.dialect_type.value = "postgresql"
    return mock


def invoke_with_mock(runner, mock_conn, args):
    def get_conn():
        return mock_conn
    return runner.invoke(cli, args, obj={"get_connection": get_conn, "env": "dev"})


class TestProcessCreate:

    @patch("nodo_etl.cli.process.ProcessRepository")
    def test_create_success(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.create.return_value = {"id": 1, "process_name": "finance_etl"}
        result = invoke_with_mock(
            runner, mock_conn,
            ["process", "create", "--name", "finance_etl", "--description", "Finance ETL process"]
        )
        assert result.exit_code == 0
        assert "created" in result.output.lower()
        repo.create.assert_called_once()

    @patch("nodo_etl.cli.process.ProcessRepository")
    def test_create_with_schedule(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.create.return_value = {"id": 1}
        repo.create_schedule.return_value = {"id": 10}
        result = invoke_with_mock(
            runner, mock_conn,
            ["process", "create", "--name", "test",
             "--schedule-name", "daily", "--cron", "0 0 * * *"]
        )
        assert result.exit_code == 0
        repo.create_schedule.assert_called_once()

    @patch("nodo_etl.cli.process.ProcessRepository")
    def test_create_duplicate_name(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.create.side_effect = Exception("Duplicate name")
        result = invoke_with_mock(
            runner, mock_conn,
            ["process", "create", "--name", "existing"]
        )
        assert "error" in result.output.lower()


class TestProcessList:

    @patch("nodo_etl.cli.process.ProcessRepository")
    def test_list_shows_processes(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.list.return_value = [
            {"id": 1, "process_name": "finance", "execution_order": 1,
             "max_parallelism": 2, "is_enabled": True},
        ]
        result = invoke_with_mock(runner, mock_conn, ["process", "list"])
        assert "finance" in result.output

    @patch("nodo_etl.cli.process.ProcessRepository")
    def test_list_enabled_filter(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.list.return_value = []
        result = invoke_with_mock(runner, mock_conn, ["process", "list", "--enabled"])
        repo.list.assert_called_once_with(is_enabled=True)


class TestProcessGet:

    @patch("nodo_etl.db.repositories.DatasetRepository.list", return_value=[
        {"dataset_name": "users", "execution_order": 1, "source_type": "database", "layer": "bronze"}
    ])
    @patch("nodo_etl.db.repositories.JobRepository.list", return_value=[
        {"id": 1, "job_name": "bronze", "execution_order": 1}
    ])
    @patch("nodo_etl.cli.process.ProcessRepository")
    def test_get_shows_details(self, MockProcRepo, mock_job_list, mock_ds_list, runner, mock_conn):
        MockProcRepo.return_value.get.return_value = {
            "id": 1, "process_name": "finance_etl"
        }
        MockProcRepo.return_value.list_schedules.return_value = [
            {"schedule_name": "daily", "cron_expression": "0 0 * * *"}
        ]
        result = invoke_with_mock(runner, mock_conn, ["process", "get", "1"])
        assert result.exit_code == 0
        assert "finance_etl" in result.output

    @patch("nodo_etl.cli.process.ProcessRepository")
    def test_get_not_found(self, MockRepo, runner, mock_conn):
        MockRepo.return_value.get.side_effect = NotFoundError("Process", 999)
        result = invoke_with_mock(runner, mock_conn, ["process", "get", "999"])
        assert "not found" in result.output.lower()


class TestProcessUpdate:

    @patch("nodo_etl.cli.process.ProcessRepository")
    def test_update_max_parallelism(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.get.return_value = {"id": 1}
        result = invoke_with_mock(
            runner, mock_conn, ["process", "update", "1", "--max-parallelism", "10"]
        )
        assert result.exit_code == 0
        repo.update.assert_called_once()


class TestProcessDelete:

    @patch("nodo_etl.cli.process.ProcessRepository")
    def test_delete_with_yes(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.get.return_value = {"id": 1}
        result = invoke_with_mock(
            runner, mock_conn, ["process", "delete", "1", "--yes"]
        )
        assert "deleted" in result.output.lower()
        repo.delete.assert_called_once()


class TestProcessEnableDisable:

    @patch("nodo_etl.cli.process.ProcessRepository")
    def test_enable(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.get.return_value = {"id": 1}
        result = invoke_with_mock(runner, mock_conn, ["process", "enable", "1"])
        assert "enabled" in result.output.lower()

    @patch("nodo_etl.cli.process.ProcessRepository")
    def test_disable(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.get.return_value = {"id": 1}
        result = invoke_with_mock(runner, mock_conn, ["process", "disable", "1"])
        assert "disabled" in result.output.lower()
