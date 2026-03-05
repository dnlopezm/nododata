"""Shared helpers for Nodo ETL Airflow DAGs."""

import logging
import os
import subprocess

import requests

logger = logging.getLogger(__name__)


def get_metadata_connection():
    """Create a MetadataDBConnection from Airflow environment."""
    from nodo_etl.config.settings import Settings
    from nodo_etl.db.connection import MetadataDBConnection

    settings = Settings()
    return MetadataDBConnection(settings)


def get_execution_repo(conn):
    """Create an ExecutionRepository from a connection."""
    from nodo_etl.db.repositories import ExecutionRepository

    return ExecutionRepository(conn)


def get_hook_repo(conn):
    """Create a HookRepository from a connection."""
    from nodo_etl.db.repositories import HookRepository

    return HookRepository(conn)


def execute_hooks(conn, entity_type, entity_id, hook_type, status=None):
    """Execute hooks for an entity.

    Args:
        conn: MetadataDBConnection
        entity_type: 'process', 'job', or 'dataset'
        entity_id: Entity ID
        hook_type: 'pre' or 'post'
        status: Current execution status (for post hooks on_status filtering)
    """
    repo = get_hook_repo(conn)
    hooks = repo.list(entity_type, entity_id)

    for hook in hooks:
        if hook.get("hook_type") != hook_type:
            continue

        # For post hooks, check on_status filter
        if hook_type == "post" and status:
            on_status = hook.get("on_status")
            if on_status and on_status != "any" and on_status != status:
                logger.info(
                    f"Skipping hook id={hook.get('id')} "
                    f"(on_status={on_status}, current={status})"
                )
                continue

        action_type = hook.get("action_type")
        action_config = hook.get("action_config", "{}")

        try:
            if action_type == "sql":
                _execute_sql_hook(conn, action_config)
            elif action_type == "api":
                _execute_api_hook(action_config)
            elif action_type == "command":
                _execute_command_hook(action_config)
            else:
                logger.warning(f"Unknown hook action_type: {action_type}")

            logger.info(f"Hook id={hook.get('id')} ({action_type}) executed successfully")
        except Exception as e:
            logger.error(f"Hook id={hook.get('id')} ({action_type}) failed: {e}")


def _execute_sql_hook(conn, action_config):
    """Execute a SQL hook."""
    import json
    config = json.loads(action_config) if isinstance(action_config, str) else action_config
    query = config.get("query", "")
    if query:
        conn.execute(query)
        logger.info(f"SQL hook executed: {query[:100]}")


def _execute_api_hook(action_config):
    """Execute an API hook."""
    import json
    config = json.loads(action_config) if isinstance(action_config, str) else action_config
    url = config.get("url", "")
    method = config.get("method", "GET").upper()
    headers = config.get("headers", {})
    body = config.get("body")

    response = requests.request(method, url, headers=headers, json=body, timeout=30)
    logger.info(f"API hook {method} {url} → {response.status_code}")
    response.raise_for_status()


def _execute_command_hook(action_config):
    """Execute a command hook."""
    import json
    config = json.loads(action_config) if isinstance(action_config, str) else action_config
    command = config.get("command", "")
    if command:
        result = subprocess.run(
            command, shell=True, capture_output=True, text=True, timeout=300
        )
        logger.info(f"Command hook: {command} → exit code {result.returncode}")
        if result.returncode != 0:
            raise RuntimeError(f"Command failed: {result.stderr}")
