"""Tests for CLI utility commands (config, tag, hook, lineage)."""

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from nodo_etl.cli.main import cli


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


class TestConfigList:

    @patch("nodo_etl.cli.utility.SystemConfigRepository")
    def test_config_list(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.list.return_value = [
            {"config_key": "timezone", "config_value": "UTC", "description": "Default timezone"},
        ]
        result = invoke_with_mock(runner, mock_conn, ["config", "list"])
        assert "timezone" in result.output


class TestConfigSet:

    @patch("nodo_etl.cli.utility.SystemConfigRepository")
    def test_config_set(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.set.return_value = {"config_key": "timezone", "config_value": "UTC"}
        result = invoke_with_mock(runner, mock_conn, ["config", "set", "timezone", "UTC"])
        assert result.exit_code == 0
        repo.set.assert_called_once_with("timezone", "UTC")

    @patch("nodo_etl.cli.utility.SystemConfigRepository")
    def test_config_set_custom_key(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.set.return_value = {"config_key": "custom", "config_value": "value"}
        result = invoke_with_mock(runner, mock_conn, ["config", "set", "custom", "value"])
        assert result.exit_code == 0


class TestTagAdd:

    @patch("nodo_etl.cli.utility.TagRepository")
    def test_tag_add_process(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.add.return_value = {"id": 1}
        result = invoke_with_mock(
            runner, mock_conn, ["tag", "add", "process", "1", "domain", "finance"]
        )
        assert result.exit_code == 0
        assert "added" in result.output.lower()

    @patch("nodo_etl.cli.utility.TagRepository")
    def test_tag_add_job(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.add.return_value = {"id": 2}
        result = invoke_with_mock(
            runner, mock_conn, ["tag", "add", "job", "1", "priority", "high"]
        )
        assert result.exit_code == 0

    @patch("nodo_etl.cli.utility.TagRepository")
    def test_tag_add_dataset(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.add.return_value = {"id": 3}
        result = invoke_with_mock(
            runner, mock_conn, ["tag", "add", "dataset", "1", "owner", "data-team"]
        )
        assert result.exit_code == 0

    def test_tag_add_invalid_entity_type(self, runner, mock_conn):
        result = invoke_with_mock(
            runner, mock_conn, ["tag", "add", "invalid", "1", "key", "value"]
        )
        assert result.exit_code != 0


class TestTagList:

    @patch("nodo_etl.cli.utility.TagRepository")
    def test_tag_list(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.list.return_value = [
            {"id": 1, "tag_key": "domain", "tag_value": "finance"},
        ]
        result = invoke_with_mock(
            runner, mock_conn, ["tag", "list", "process", "1"]
        )
        assert "domain" in result.output


class TestHookAdd:

    @patch("nodo_etl.cli.utility.HookRepository")
    def test_hook_add(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.create.return_value = {"id": 1}
        result = invoke_with_mock(
            runner, mock_conn,
            ["hook", "add", "--entity-type", "process", "--entity-id", "1",
             "--hook-type", "pre", "--action-type", "sql",
             "--action-config", '{"query": "SELECT 1"}']
        )
        assert result.exit_code == 0
        assert "created" in result.output.lower()


class TestHookList:

    @patch("nodo_etl.cli.utility.HookRepository")
    def test_hook_list(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.list.return_value = [
            {"id": 1, "hook_type": "pre", "action_type": "sql",
             "execution_order": 1, "is_enabled": True},
        ]
        result = invoke_with_mock(
            runner, mock_conn, ["hook", "list", "process", "1"]
        )
        assert result.exit_code == 0


class TestLineageAdd:

    @patch("nodo_etl.cli.utility.LineageRepository")
    def test_lineage_add(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.add.return_value = {"id": 1}
        result = invoke_with_mock(runner, mock_conn, ["lineage", "add", "1", "2"])
        assert result.exit_code == 0
        assert "created" in result.output.lower()

    def test_lineage_add_self_reference(self, runner, mock_conn):
        result = invoke_with_mock(runner, mock_conn, ["lineage", "add", "1", "1"])
        assert "self-referencing" in result.output.lower() or "error" in result.output.lower()


class TestLineageShow:

    @patch("nodo_etl.cli.utility.LineageRepository")
    def test_lineage_show(self, MockRepo, runner, mock_conn):
        repo = MockRepo.return_value
        repo.get_upstream.return_value = [
            {"source_dataset_id": 1, "source_dataset_name": "raw_users"},
        ]
        repo.get_downstream.return_value = [
            {"target_dataset_id": 3, "target_dataset_name": "gold_users"},
        ]
        result = invoke_with_mock(runner, mock_conn, ["lineage", "show", "2"])
        assert "raw_users" in result.output
        assert "gold_users" in result.output
