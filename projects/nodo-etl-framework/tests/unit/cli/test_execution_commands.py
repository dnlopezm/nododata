"""Tests for CLI execution commands."""

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


class TestRunProcess:

    @patch("nodo_etl.cli.execution.ExecutionRepository")
    def test_run_process_success(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.start_process_execution.return_value = 42
        result = invoke_with_mock(runner, mock_conn, ["run", "process", "1"])
        assert result.exit_code == 0
        assert "42" in result.output

    @patch("nodo_etl.cli.execution.ExecutionRepository")
    def test_run_process_not_found(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.start_process_execution.side_effect = NotFoundError("Process", 999)
        result = invoke_with_mock(runner, mock_conn, ["run", "process", "999"])
        assert "not found" in result.output.lower()


class TestRunJob:

    @patch("nodo_etl.cli.execution.ExecutionRepository")
    def test_run_job_success(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        result = invoke_with_mock(runner, mock_conn, ["run", "job", "1"])
        assert result.exit_code == 0
        assert "started" in result.output.lower()


class TestRunDataset:

    @patch("nodo_etl.cli.execution.ExecutionRepository")
    def test_run_dataset_success(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        result = invoke_with_mock(runner, mock_conn, ["run", "dataset", "1"])
        assert result.exit_code == 0
        assert "started" in result.output.lower()


class TestStatus:

    @patch("nodo_etl.cli.execution.ExecutionRepository")
    def test_status_shows_summary(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.get_execution_summary.return_value = [
            {
                "process_execution_id": 1,
                "process_name": "finance",
                "process_status": "success",
                "job_name": "bronze",
                "job_status": "success",
                "dataset_name": "users",
                "dataset_status": "success",
                "dataset_rows_read": 1000,
                "dataset_rows_written": 950,
                "dataset_duration_seconds": 30,
            }
        ]
        result = invoke_with_mock(runner, mock_conn, ["status", "1"])
        assert result.exit_code == 0

    @patch("nodo_etl.cli.execution.ExecutionRepository")
    def test_status_not_found(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.get_execution_summary.return_value = []
        result = invoke_with_mock(runner, mock_conn, ["status", "999"])
        assert "not found" in result.output.lower()


class TestHistory:

    @patch("nodo_etl.cli.execution.ExecutionRepository")
    def test_history_shows_list(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.get_execution_summary.return_value = [
            {
                "process_execution_id": 1,
                "process_status": "success",
                "process_environment": "dev",
                "process_start_time": "2024-01-01",
                "process_end_time": "2024-01-01",
                "process_total_jobs": 2,
                "process_completed_jobs": 2,
                "process_failed_jobs": 0,
            },
        ]
        result = invoke_with_mock(
            runner, mock_conn, ["history", "--process-id", "1"]
        )
        assert result.exit_code == 0

    @patch("nodo_etl.cli.execution.ExecutionRepository")
    def test_history_limit(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.get_execution_summary.return_value = [
            {"process_execution_id": i, "process_status": "success",
             "process_environment": "dev", "process_start_time": f"2024-01-0{i+1}",
             "process_end_time": f"2024-01-0{i+1}", "process_total_jobs": 1,
             "process_completed_jobs": 1, "process_failed_jobs": 0}
            for i in range(8)
        ]
        result = invoke_with_mock(
            runner, mock_conn, ["history", "--process-id", "1", "--limit", "5"]
        )
        assert result.exit_code == 0


class TestRetry:

    @patch("nodo_etl.cli.execution.ExecutionRepository")
    def test_retry_success(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.retry_failed_datasets.return_value = [
            {"dataset_execution_id": 5, "dataset_name": "users", "retry_count": 2},
        ]
        result = invoke_with_mock(runner, mock_conn, ["retry", "1"])
        assert result.exit_code == 0
        assert "retrying" in result.output.lower()

    @patch("nodo_etl.cli.execution.ExecutionRepository")
    def test_retry_nothing_to_retry(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.retry_failed_datasets.return_value = []
        result = invoke_with_mock(runner, mock_conn, ["retry", "1"])
        assert "nothing to retry" in result.output.lower()
