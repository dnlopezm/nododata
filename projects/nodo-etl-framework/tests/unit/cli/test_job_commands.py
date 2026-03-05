"""Tests for CLI job commands."""

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


class TestJobCreate:

    @patch("nodo_etl.cli.job.JobRepository")
    def test_create_success(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.create.return_value = {"id": 1, "job_name": "bronze_ingestion"}
        result = invoke_with_mock(
            runner, mock_conn,
            ["job", "create", "--process-id", "1", "--name", "bronze_ingestion", "--order", "1"]
        )
        assert result.exit_code == 0
        assert "created" in result.output.lower()

    @patch("nodo_etl.cli.job.JobRepository")
    def test_create_invalid_process_id(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.create.side_effect = Exception("Foreign key violation")
        result = invoke_with_mock(
            runner, mock_conn,
            ["job", "create", "--process-id", "999", "--name", "test"]
        )
        assert "error" in result.output.lower()

    @patch("nodo_etl.cli.job.JobRepository")
    def test_create_duplicate_name(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.create.side_effect = Exception("Duplicate name")
        result = invoke_with_mock(
            runner, mock_conn,
            ["job", "create", "--process-id", "1", "--name", "existing"]
        )
        assert "error" in result.output.lower()


class TestJobList:

    @patch("nodo_etl.cli.job.JobRepository")
    def test_list_shows_jobs(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.list.return_value = [
            {"id": 1, "job_name": "bronze", "execution_order": 1,
             "max_parallelism": 2, "is_enabled": True},
        ]
        result = invoke_with_mock(runner, mock_conn, ["job", "list", "--process-id", "1"])
        assert "bronze" in result.output

    def test_list_without_process_id(self, runner, mock_conn):
        result = invoke_with_mock(runner, mock_conn, ["job", "list"])
        assert result.exit_code != 0


class TestJobGet:

    @patch("nodo_etl.cli.job.DatasetRepository")
    @patch("nodo_etl.cli.job.JobRepository")
    def test_get_shows_details(self, MockJobRepo, MockDsRepo, runner, mock_conn):
        MockJobRepo.return_value.get.return_value = {
            "id": 1, "job_name": "bronze_ingestion"
        }
        MockDsRepo.return_value.list.return_value = [
            {"dataset_name": "users", "execution_order": 1,
             "source_type": "database", "layer": "bronze", "load_strategy": "full"}
        ]
        result = invoke_with_mock(runner, mock_conn, ["job", "get", "1"])
        assert "bronze_ingestion" in result.output

    @patch("nodo_etl.cli.job.JobRepository")
    def test_get_not_found(self, MockRepo, runner, mock_conn):
        MockRepo.return_value.get.side_effect = NotFoundError("Job", 999)
        result = invoke_with_mock(runner, mock_conn, ["job", "get", "999"])
        assert "not found" in result.output.lower()


class TestJobUpdate:

    @patch("nodo_etl.cli.job.JobRepository")
    def test_update_order(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.get.return_value = {"id": 1}
        result = invoke_with_mock(
            runner, mock_conn, ["job", "update", "1", "--order", "2"]
        )
        assert result.exit_code == 0
        repo.update.assert_called_once()


class TestJobDelete:

    @patch("nodo_etl.cli.job.JobRepository")
    def test_delete_with_yes(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.get.return_value = {"id": 1}
        result = invoke_with_mock(
            runner, mock_conn, ["job", "delete", "1", "--yes"]
        )
        assert "deleted" in result.output.lower()


class TestJobEnableDisable:

    @patch("nodo_etl.cli.job.JobRepository")
    def test_enable(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.get.return_value = {"id": 1}
        result = invoke_with_mock(runner, mock_conn, ["job", "enable", "1"])
        assert "enabled" in result.output.lower()

    @patch("nodo_etl.cli.job.JobRepository")
    def test_disable(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.get.return_value = {"id": 1}
        result = invoke_with_mock(runner, mock_conn, ["job", "disable", "1"])
        assert "disabled" in result.output.lower()
