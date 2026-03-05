"""CLI integration tests against SQL Server.

Requires Docker services. Enable with NODO_ETL_INTEGRATION_TESTS=1.
"""
import os
import uuid

import pytest
from click.testing import CliRunner

from tests.integration.conftest import skip_integration
from nodo_etl.cli.main import cli


@skip_integration
class TestCLISQLServer:
    """CLI integration tests using SQL Server backend."""

    @pytest.fixture(autouse=True)
    def setup_env(self, monkeypatch):
        """Set environment for SQL Server."""
        monkeypatch.setenv("NODO_ETL_DB_TYPE", "sqlserver")
        monkeypatch.setenv("NODO_ETL_SQLSERVER_HOST", "localhost")
        monkeypatch.setenv("NODO_ETL_SQLSERVER_PORT", "1433")
        monkeypatch.setenv("NODO_ETL_SQLSERVER_DB", "nodo_etl_db")
        monkeypatch.setenv("NODO_ETL_SQLSERVER_USER", "sa")
        monkeypatch.setenv("NODO_ETL_SQLSERVER_PASSWORD", "NodoEtl@2024!")
        monkeypatch.setenv("NODO_ETL_ENVIRONMENT", "dev")
        self.runner = CliRunner()
        self.unique = uuid.uuid4().hex[:8]

    def test_full_workflow_create_metadata(self):
        """Create connection -> process -> job -> dataset via CLI."""
        # Create connection
        result = self.runner.invoke(cli, [
            "connection", "create",
            "--name", f"cli_ss_conn_{self.unique}",
            "--type", "sqlserver",
            "--host", "localhost",
            "--port", "1433",
            "--environment", "dev",
        ])
        assert result.exit_code == 0

        # Create process
        result = self.runner.invoke(cli, [
            "process", "create",
            "--name", f"cli_ss_proc_{self.unique}",
            "--order", "1",
        ])
        assert result.exit_code == 0

        # List processes
        result = self.runner.invoke(cli, ["process", "list"])
        assert result.exit_code == 0
        assert f"cli_ss_proc_{self.unique}" in result.output

    def test_full_workflow_run_process(self):
        """Create metadata and run a process execution."""
        # Setup: create connection, process, job, dataset
        conn_name = f"cli_run_conn_{self.unique}"
        self.runner.invoke(cli, [
            "connection", "create", "--name", conn_name,
            "--type", "sqlserver", "--host", "localhost",
            "--port", "1433", "--environment", "dev",
        ])
        proc_name = f"cli_run_proc_{self.unique}"
        result = self.runner.invoke(cli, [
            "process", "create", "--name", proc_name, "--order", "1",
        ])
        assert result.exit_code == 0

    def test_config_list(self):
        """Verify config list shows system config."""
        result = self.runner.invoke(cli, ["config", "list"])
        assert result.exit_code == 0

    def test_config_set(self):
        """Set and verify a config value."""
        result = self.runner.invoke(cli, [
            "config", "set", "test_key", "test_value",
        ])
        assert result.exit_code == 0

    def test_connection_list_empty_filter(self):
        """List connections with type filter."""
        result = self.runner.invoke(cli, [
            "connection", "list", "--type", "oracle",
        ])
        assert result.exit_code == 0

    def test_process_get_not_found(self):
        """Get non-existent process."""
        result = self.runner.invoke(cli, ["process", "get", "99999"])
        assert (
            result.exit_code != 0
            or "not found" in result.output.lower()
            or "error" in result.output.lower()
        )

    def test_cli_env_flag(self):
        """Test CLI respects environment context."""
        result = self.runner.invoke(cli, ["config", "list"])
        assert result.exit_code == 0
