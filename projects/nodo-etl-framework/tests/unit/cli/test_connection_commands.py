"""Tests for CLI connection commands."""

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
    """Mock the get_connection to return a mock MetadataDBConnection."""
    mock = MagicMock()
    mock.schema_name = "nodo_etl"
    mock.dialect = MagicMock()
    mock.dialect.dialect_type = MagicMock()
    mock.dialect.dialect_type.value = "postgresql"
    return mock


def invoke_with_mock(runner, mock_conn, args):
    """Helper to invoke CLI with mocked connection."""
    def get_conn():
        return mock_conn
    return runner.invoke(cli, args, obj={"get_connection": get_conn, "env": "dev"})


class TestConnectionCreate:

    @patch("nodo_etl.cli.connection.ConnectionRepository")
    def test_create_with_all_flags(self, MockRepo, runner, mock_conn):
        repo_instance = MockRepo.return_value
        repo_instance.create.return_value = {"id": 1, "connection_name": "test"}
        result = invoke_with_mock(
            runner, mock_conn,
            ["connection", "create", "--name", "test", "--type", "postgresql",
             "--host", "localhost", "--port", "5432"]
        )
        assert result.exit_code == 0
        assert "created" in result.output.lower() or "id=1" in result.output
        repo_instance.create.assert_called_once()

    @patch("nodo_etl.cli.connection.ConnectionRepository")
    def test_create_missing_required_flag(self, MockRepo, runner, mock_conn):
        result = invoke_with_mock(
            runner, mock_conn,
            ["connection", "create", "--name", "test"]
        )
        assert result.exit_code != 0

    @patch("nodo_etl.cli.connection.ConnectionRepository")
    def test_create_invalid_type(self, MockRepo, runner, mock_conn):
        result = invoke_with_mock(
            runner, mock_conn,
            ["connection", "create", "--name", "test", "--type", "invalid_db"]
        )
        assert result.exit_code != 0


class TestConnectionList:

    @patch("nodo_etl.cli.connection.ConnectionRepository")
    def test_list_shows_connections(self, MockRepo, runner, mock_conn):
        repo_instance = MockRepo.return_value
        repo_instance.list.return_value = [
            {"id": 1, "connection_name": "pg_conn", "connection_type": "postgresql",
             "host": "localhost", "port": 5432, "environment": "dev"},
        ]
        result = invoke_with_mock(runner, mock_conn, ["connection", "list"])
        assert result.exit_code == 0
        assert "pg_conn" in result.output

    @patch("nodo_etl.cli.connection.ConnectionRepository")
    def test_list_filter_by_type(self, MockRepo, runner, mock_conn):
        repo_instance = MockRepo.return_value
        repo_instance.list.return_value = []
        result = invoke_with_mock(
            runner, mock_conn, ["connection", "list", "--type", "sqlserver"]
        )
        repo_instance.list.assert_called_once_with(connection_type="sqlserver", environment=None)

    @patch("nodo_etl.cli.connection.ConnectionRepository")
    def test_list_filter_by_environment(self, MockRepo, runner, mock_conn):
        repo_instance = MockRepo.return_value
        repo_instance.list.return_value = []
        result = invoke_with_mock(
            runner, mock_conn, ["connection", "list", "--environment", "prod"]
        )
        repo_instance.list.assert_called_once_with(connection_type=None, environment="prod")

    @patch("nodo_etl.cli.connection.ConnectionRepository")
    def test_list_no_connections(self, MockRepo, runner, mock_conn):
        repo_instance = MockRepo.return_value
        repo_instance.list.return_value = []
        result = invoke_with_mock(runner, mock_conn, ["connection", "list"])
        assert "no connections found" in result.output.lower()


class TestConnectionGet:

    @patch("nodo_etl.cli.connection.ConnectionRepository")
    def test_get_shows_details(self, MockRepo, runner, mock_conn):
        repo_instance = MockRepo.return_value
        repo_instance.get.return_value = {
            "id": 1, "connection_name": "test", "connection_type": "postgresql",
            "host": "localhost", "secret_reference": "MY_SECRET_VAR",
        }
        result = invoke_with_mock(runner, mock_conn, ["connection", "get", "1"])
        assert result.exit_code == 0
        assert "test" in result.output
        # Secret should be masked
        assert "MY_SECRET_VAR" not in result.output

    @patch("nodo_etl.cli.connection.ConnectionRepository")
    def test_get_not_found(self, MockRepo, runner, mock_conn):
        repo_instance = MockRepo.return_value
        repo_instance.get.side_effect = NotFoundError("Connection", 999)
        result = invoke_with_mock(runner, mock_conn, ["connection", "get", "999"])
        assert "not found" in result.output.lower()


class TestConnectionUpdate:

    @patch("nodo_etl.cli.connection.ConnectionRepository")
    def test_update_host(self, MockRepo, runner, mock_conn):
        repo_instance = MockRepo.return_value
        repo_instance.get.return_value = {"id": 1}
        result = invoke_with_mock(
            runner, mock_conn, ["connection", "update", "1", "--host", "new-host"]
        )
        assert result.exit_code == 0
        repo_instance.update.assert_called_once()


class TestConnectionDelete:

    @patch("nodo_etl.cli.connection.ConnectionRepository")
    def test_delete_with_yes(self, MockRepo, runner, mock_conn):
        repo_instance = MockRepo.return_value
        repo_instance.get.return_value = {"id": 1}
        result = invoke_with_mock(
            runner, mock_conn, ["connection", "delete", "1", "--yes"]
        )
        assert result.exit_code == 0
        repo_instance.delete.assert_called_once()
        assert "deleted" in result.output.lower()

    @patch("nodo_etl.cli.connection.ConnectionRepository")
    def test_delete_not_found(self, MockRepo, runner, mock_conn):
        repo_instance = MockRepo.return_value
        repo_instance.get.side_effect = NotFoundError("Connection", 999)
        result = invoke_with_mock(
            runner, mock_conn, ["connection", "delete", "999", "--yes"]
        )
        assert "not found" in result.output.lower()


class TestConnectionTest:

    @patch("nodo_etl.cli.connection.ConnectionRepository")
    def test_test_success(self, MockRepo, runner, mock_conn):
        repo_instance = MockRepo.return_value
        repo_instance.get.return_value = {"id": 1}
        result = invoke_with_mock(runner, mock_conn, ["connection", "test", "1"])
        assert result.exit_code == 0
        assert "reachable" in result.output.lower()
