"""Tests for CLI dataset commands."""

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


class TestDatasetCreate:

    @patch("nodo_etl.cli.dataset.DatasetRepository")
    def test_create_database_source(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.create.return_value = {"id": 1}
        result = invoke_with_mock(
            runner, mock_conn,
            ["dataset", "create", "--job-id", "1", "--name", "users",
             "--source-type", "database", "--layer", "bronze",
             "--source-schema", "dbo", "--source-table", "users"]
        )
        assert result.exit_code == 0
        assert "created" in result.output.lower()

    @patch("nodo_etl.cli.dataset.DatasetRepository")
    def test_create_file_source(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.create.return_value = {"id": 2}
        result = invoke_with_mock(
            runner, mock_conn,
            ["dataset", "create", "--job-id", "1", "--name", "events",
             "--source-type", "file", "--layer", "bronze",
             "--file-path", "/data/events.csv", "--file-format", "csv"]
        )
        assert result.exit_code == 0

    @patch("nodo_etl.cli.dataset.DatasetRepository")
    def test_create_api_source(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.create.return_value = {"id": 3}
        result = invoke_with_mock(
            runner, mock_conn,
            ["dataset", "create", "--job-id", "1", "--name", "weather",
             "--source-type", "api", "--layer", "bronze",
             "--api-url", "https://api.example.com/data", "--api-method", "GET"]
        )
        assert result.exit_code == 0

    @patch("nodo_etl.cli.dataset.DatasetRepository")
    def test_create_stream_source(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.create.return_value = {"id": 4}
        result = invoke_with_mock(
            runner, mock_conn,
            ["dataset", "create", "--job-id", "1", "--name", "clicks",
             "--source-type", "stream", "--layer", "bronze"]
        )
        assert result.exit_code == 0

    def test_create_invalid_load_strategy(self, runner, mock_conn):
        result = invoke_with_mock(
            runner, mock_conn,
            ["dataset", "create", "--job-id", "1", "--name", "test",
             "--source-type", "database", "--layer", "bronze",
             "--load-strategy", "invalid"]
        )
        assert result.exit_code != 0

    def test_create_invalid_layer(self, runner, mock_conn):
        result = invoke_with_mock(
            runner, mock_conn,
            ["dataset", "create", "--job-id", "1", "--name", "test",
             "--source-type", "database", "--layer", "invalid"]
        )
        assert result.exit_code != 0


class TestDatasetList:

    @patch("nodo_etl.cli.dataset.DatasetRepository")
    def test_list_shows_datasets(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.list.return_value = [
            {"id": 1, "dataset_name": "users", "source_type": "database",
             "layer": "bronze", "load_strategy": "full", "execution_order": 1, "is_enabled": True},
        ]
        result = invoke_with_mock(runner, mock_conn, ["dataset", "list", "--job-id", "1"])
        assert "users" in result.output


class TestDatasetGet:

    @patch("nodo_etl.cli.dataset.DatasetRepository")
    def test_get_shows_details(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.get.return_value = {
            "id": 1, "dataset_name": "users", "source_type": "database",
            "source_config": {"source_schema": "dbo", "source_table": "users"},
        }
        result = invoke_with_mock(runner, mock_conn, ["dataset", "get", "1"])
        assert "users" in result.output
        assert "source_schema" in result.output.lower() or "dbo" in result.output

    @patch("nodo_etl.cli.dataset.DatasetRepository")
    def test_get_not_found(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.get.side_effect = NotFoundError("Dataset", 999)
        result = invoke_with_mock(runner, mock_conn, ["dataset", "get", "999"])
        assert "not found" in result.output.lower()


class TestDatasetUpdate:

    @patch("nodo_etl.cli.dataset.DatasetRepository")
    def test_update_load_strategy(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.get.return_value = {"id": 1}
        result = invoke_with_mock(
            runner, mock_conn,
            ["dataset", "update", "1", "--load-strategy", "incremental"]
        )
        assert result.exit_code == 0
        repo.update.assert_called_once()


class TestDatasetDelete:

    @patch("nodo_etl.cli.dataset.DatasetRepository")
    def test_delete_with_yes(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.get.return_value = {"id": 1}
        result = invoke_with_mock(
            runner, mock_conn, ["dataset", "delete", "1", "--yes"]
        )
        assert "deleted" in result.output.lower()


class TestDatasetEnableDisable:

    @patch("nodo_etl.cli.dataset.DatasetRepository")
    def test_enable(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.get.return_value = {"id": 1}
        result = invoke_with_mock(runner, mock_conn, ["dataset", "enable", "1"])
        assert "enabled" in result.output.lower()

    @patch("nodo_etl.cli.dataset.DatasetRepository")
    def test_disable(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.get.return_value = {"id": 1}
        result = invoke_with_mock(runner, mock_conn, ["dataset", "disable", "1"])
        assert "disabled" in result.output.lower()
