"""Repository classes for ETL metadata CRUD operations."""

from datetime import datetime, timezone
from typing import Any

from nodo_etl.db.connection import MetadataDBConnection


class RepositoryError(Exception):
    """Base repository error."""


class NotFoundError(RepositoryError):
    """Raised when an entity is not found."""

    def __init__(self, entity_type: str, entity_id: int) -> None:
        self.entity_type = entity_type
        self.entity_id = entity_id
        super().__init__(f"{entity_type} with id={entity_id} not found")


class DuplicateError(RepositoryError):
    """Raised when a unique constraint would be violated."""


class BaseRepository:
    """Base repository with shared functionality."""

    def __init__(self, connection: MetadataDBConnection) -> None:
        self._conn = connection
        self._schema = connection.schema_name

    def _table(self, name: str) -> str:
        return f"{self._schema}.{name}"

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)

    def _audit_fields(self, user: str = "system") -> dict:
        now = self._now()
        return {
            "created_at": now,
            "created_by": user,
            "updated_at": now,
            "updated_by": user,
        }

    def _update_audit(self, user: str = "system") -> dict:
        return {
            "updated_at": self._now(),
            "updated_by": user,
        }


class ProcessRepository(BaseRepository):
    """CRUD operations for etl_process and etl_schedule."""

    def create(self, data: dict, user: str = "system") -> dict:
        """Create a new process."""
        fields = {**data, **self._audit_fields(user)}
        fields.setdefault("is_enabled", True)
        fields.setdefault("is_deleted", False)
        cols = ", ".join(fields.keys())
        vals = ", ".join(f":{k}" for k in fields.keys())
        sql = (
            f"INSERT INTO {self._table('etl_process')} ({cols}) "
            f"VALUES ({vals}) RETURNING *"
        )
        if self._conn.dialect.dialect_type.value == "sqlserver":
            sql = (
                f"INSERT INTO {self._table('etl_process')} ({cols}) "
                f"OUTPUT INSERTED.* VALUES ({vals})"
            )
        rows = self._conn.execute(sql, fields)
        return rows[0] if rows else {}

    def get(self, process_id: int) -> dict:
        """Get a process by ID. Raises NotFoundError if not found or soft-deleted."""
        sql = (
            f"SELECT * FROM {self._table('etl_process')} "
            f"WHERE id = :id AND is_deleted = :is_deleted"
        )
        rows = self._conn.execute(sql, {"id": process_id, "is_deleted": False})
        if not rows:
            raise NotFoundError("Process", process_id)
        return rows[0]

    def list(self, is_enabled: bool | None = None) -> list[dict]:
        """List all non-deleted processes, optionally filtered by enabled status."""
        sql = f"SELECT * FROM {self._table('etl_process')} WHERE is_deleted = :is_deleted"
        params: dict[str, Any] = {"is_deleted": False}
        if is_enabled is not None:
            sql += " AND is_enabled = :is_enabled"
            params["is_enabled"] = is_enabled
        sql += " ORDER BY execution_order, id"
        return self._conn.execute(sql, params)

    def update(self, process_id: int, data: dict, user: str = "system") -> dict:
        """Update a process."""
        self.get(process_id)  # Verify exists
        fields = {**data, **self._update_audit(user)}
        set_clause = ", ".join(f"{k} = :{k}" for k in fields.keys())
        fields["id"] = process_id
        sql = (
            f"UPDATE {self._table('etl_process')} "
            f"SET {set_clause} WHERE id = :id"
        )
        self._conn.execute(sql, fields)
        return self.get(process_id)

    def delete(self, process_id: int, user: str = "system") -> None:
        """Soft delete a process and cascade to jobs and datasets."""
        self.get(process_id)  # Verify exists
        audit = self._update_audit(user)
        # Soft delete datasets under this process's jobs
        sql = (
            f"UPDATE {self._table('etl_dataset')} "
            f"SET is_deleted = :is_deleted, updated_at = :updated_at, updated_by = :updated_by "
            f"WHERE job_id IN (SELECT id FROM {self._table('etl_job')} WHERE process_id = :process_id)"
        )
        self._conn.execute(sql, {"is_deleted": True, **audit, "process_id": process_id})
        # Soft delete jobs
        sql = (
            f"UPDATE {self._table('etl_job')} "
            f"SET is_deleted = :is_deleted, updated_at = :updated_at, updated_by = :updated_by "
            f"WHERE process_id = :process_id"
        )
        self._conn.execute(sql, {"is_deleted": True, **audit, "process_id": process_id})
        # Soft delete process
        sql = (
            f"UPDATE {self._table('etl_process')} "
            f"SET is_deleted = :is_deleted, updated_at = :updated_at, updated_by = :updated_by "
            f"WHERE id = :id"
        )
        self._conn.execute(sql, {"is_deleted": True, **audit, "id": process_id})

    def create_schedule(self, data: dict, user: str = "system") -> dict:
        """Create a schedule for a process."""
        fields = {**data, **self._audit_fields(user)}
        fields.setdefault("is_enabled", True)
        fields.setdefault("is_deleted", False)
        cols = ", ".join(fields.keys())
        vals = ", ".join(f":{k}" for k in fields.keys())
        sql = (
            f"INSERT INTO {self._table('etl_schedule')} ({cols}) "
            f"VALUES ({vals}) RETURNING *"
        )
        if self._conn.dialect.dialect_type.value == "sqlserver":
            sql = (
                f"INSERT INTO {self._table('etl_schedule')} ({cols}) "
                f"OUTPUT INSERTED.* VALUES ({vals})"
            )
        rows = self._conn.execute(sql, fields)
        return rows[0] if rows else {}

    def list_schedules(self, process_id: int) -> list[dict]:
        """List all non-deleted schedules for a process."""
        sql = (
            f"SELECT * FROM {self._table('etl_schedule')} "
            f"WHERE process_id = :process_id AND is_deleted = :is_deleted "
            f"ORDER BY id"
        )
        return self._conn.execute(sql, {"process_id": process_id, "is_deleted": False})

    def delete_schedule(self, schedule_id: int, user: str = "system") -> None:
        """Soft delete a schedule."""
        audit = self._update_audit(user)
        sql = (
            f"UPDATE {self._table('etl_schedule')} "
            f"SET is_deleted = :is_deleted, updated_at = :updated_at, updated_by = :updated_by "
            f"WHERE id = :id"
        )
        self._conn.execute(sql, {"is_deleted": True, **audit, "id": schedule_id})


class JobRepository(BaseRepository):
    """CRUD operations for etl_job."""

    def create(self, data: dict, user: str = "system") -> dict:
        """Create a new job."""
        fields = {**data, **self._audit_fields(user)}
        fields.setdefault("is_enabled", True)
        fields.setdefault("is_deleted", False)
        cols = ", ".join(fields.keys())
        vals = ", ".join(f":{k}" for k in fields.keys())
        sql = (
            f"INSERT INTO {self._table('etl_job')} ({cols}) "
            f"VALUES ({vals}) RETURNING *"
        )
        if self._conn.dialect.dialect_type.value == "sqlserver":
            sql = (
                f"INSERT INTO {self._table('etl_job')} ({cols}) "
                f"OUTPUT INSERTED.* VALUES ({vals})"
            )
        rows = self._conn.execute(sql, fields)
        return rows[0] if rows else {}

    def get(self, job_id: int) -> dict:
        """Get a job by ID."""
        sql = (
            f"SELECT * FROM {self._table('etl_job')} "
            f"WHERE id = :id AND is_deleted = :is_deleted"
        )
        rows = self._conn.execute(sql, {"id": job_id, "is_deleted": False})
        if not rows:
            raise NotFoundError("Job", job_id)
        return rows[0]

    def list(self, process_id: int) -> list[dict]:
        """List jobs for a process, ordered by execution_order."""
        sql = (
            f"SELECT * FROM {self._table('etl_job')} "
            f"WHERE process_id = :process_id AND is_deleted = :is_deleted "
            f"ORDER BY execution_order, id"
        )
        return self._conn.execute(sql, {"process_id": process_id, "is_deleted": False})

    def update(self, job_id: int, data: dict, user: str = "system") -> dict:
        """Update a job."""
        self.get(job_id)
        fields = {**data, **self._update_audit(user)}
        set_clause = ", ".join(f"{k} = :{k}" for k in fields.keys())
        fields["id"] = job_id
        sql = (
            f"UPDATE {self._table('etl_job')} "
            f"SET {set_clause} WHERE id = :id"
        )
        self._conn.execute(sql, fields)
        return self.get(job_id)

    def delete(self, job_id: int, user: str = "system") -> None:
        """Soft delete a job and cascade to datasets."""
        self.get(job_id)
        audit = self._update_audit(user)
        # Soft delete datasets
        sql = (
            f"UPDATE {self._table('etl_dataset')} "
            f"SET is_deleted = :is_deleted, updated_at = :updated_at, updated_by = :updated_by "
            f"WHERE job_id = :job_id"
        )
        self._conn.execute(sql, {"is_deleted": True, **audit, "job_id": job_id})
        # Soft delete job
        sql = (
            f"UPDATE {self._table('etl_job')} "
            f"SET is_deleted = :is_deleted, updated_at = :updated_at, updated_by = :updated_by "
            f"WHERE id = :id"
        )
        self._conn.execute(sql, {"is_deleted": True, **audit, "id": job_id})


class DatasetRepository(BaseRepository):
    """CRUD operations for etl_dataset and source config tables."""

    _SOURCE_CONFIG_TABLES = {
        "database": "etl_dataset_db_config",
        "file": "etl_dataset_file_config",
        "api": "etl_dataset_api_config",
        "stream": "etl_dataset_stream_config",
    }

    def create(
        self,
        data: dict,
        source_config: dict | None = None,
        user: str = "system",
    ) -> dict:
        """Create a dataset with optional source config."""
        source_type = data.get("source_type")
        if source_type and source_type in self._SOURCE_CONFIG_TABLES and not source_config:
            raise RepositoryError(
                f"source_config required for source_type='{source_type}'"
            )

        fields = {**data, **self._audit_fields(user)}
        fields.setdefault("is_enabled", True)
        fields.setdefault("is_deleted", False)
        cols = ", ".join(fields.keys())
        vals = ", ".join(f":{k}" for k in fields.keys())

        sql = (
            f"INSERT INTO {self._table('etl_dataset')} ({cols}) "
            f"VALUES ({vals}) RETURNING *"
        )
        if self._conn.dialect.dialect_type.value == "sqlserver":
            sql = (
                f"INSERT INTO {self._table('etl_dataset')} ({cols}) "
                f"OUTPUT INSERTED.* VALUES ({vals})"
            )

        rows = self._conn.execute(sql, fields)
        dataset = rows[0] if rows else {}

        # Create source config if provided
        if source_config and source_type in self._SOURCE_CONFIG_TABLES:
            config_table = self._SOURCE_CONFIG_TABLES[source_type]
            config_fields = {
                **source_config,
                "dataset_id": dataset["id"],
                **self._audit_fields(user),
            }
            config_cols = ", ".join(config_fields.keys())
            config_vals = ", ".join(f":{k}" for k in config_fields.keys())
            config_sql = (
                f"INSERT INTO {self._table(config_table)} ({config_cols}) "
                f"VALUES ({config_vals})"
            )
            self._conn.execute(config_sql, config_fields)

        return dataset

    def get(self, dataset_id: int) -> dict:
        """Get a dataset by ID with source config."""
        sql = (
            f"SELECT * FROM {self._table('etl_dataset')} "
            f"WHERE id = :id AND is_deleted = :is_deleted"
        )
        rows = self._conn.execute(sql, {"id": dataset_id, "is_deleted": False})
        if not rows:
            raise NotFoundError("Dataset", dataset_id)

        dataset = rows[0]
        source_type = dataset.get("source_type")
        if source_type and source_type in self._SOURCE_CONFIG_TABLES:
            config_table = self._SOURCE_CONFIG_TABLES[source_type]
            config_sql = (
                f"SELECT * FROM {self._table(config_table)} "
                f"WHERE dataset_id = :dataset_id"
            )
            config_rows = self._conn.execute(config_sql, {"dataset_id": dataset_id})
            if config_rows:
                dataset["source_config"] = config_rows[0]

        return dataset

    def list(self, job_id: int) -> list[dict]:
        """List datasets for a job, ordered by execution_order."""
        sql = (
            f"SELECT * FROM {self._table('etl_dataset')} "
            f"WHERE job_id = :job_id AND is_deleted = :is_deleted "
            f"ORDER BY execution_order, id"
        )
        return self._conn.execute(sql, {"job_id": job_id, "is_deleted": False})

    def update(
        self,
        dataset_id: int,
        data: dict,
        source_config: dict | None = None,
        user: str = "system",
    ) -> dict:
        """Update a dataset and optionally its source config."""
        existing = self.get(dataset_id)
        fields = {**data, **self._update_audit(user)}
        set_clause = ", ".join(f"{k} = :{k}" for k in fields.keys())
        fields["id"] = dataset_id
        sql = (
            f"UPDATE {self._table('etl_dataset')} "
            f"SET {set_clause} WHERE id = :id"
        )
        self._conn.execute(sql, fields)

        if source_config:
            source_type = data.get("source_type", existing.get("source_type"))
            if source_type in self._SOURCE_CONFIG_TABLES:
                config_table = self._SOURCE_CONFIG_TABLES[source_type]
                config_fields = {**source_config, **self._update_audit(user)}
                config_set = ", ".join(f"{k} = :{k}" for k in config_fields.keys())
                config_fields["dataset_id"] = dataset_id
                config_sql = (
                    f"UPDATE {self._table(config_table)} "
                    f"SET {config_set} WHERE dataset_id = :dataset_id"
                )
                self._conn.execute(config_sql, config_fields)

        return self.get(dataset_id)

    def delete(self, dataset_id: int, user: str = "system") -> None:
        """Soft delete a dataset."""
        existing = self.get(dataset_id)
        audit = self._update_audit(user)
        sql = (
            f"UPDATE {self._table('etl_dataset')} "
            f"SET is_deleted = :is_deleted, updated_at = :updated_at, updated_by = :updated_by "
            f"WHERE id = :id"
        )
        self._conn.execute(sql, {"is_deleted": True, **audit, "id": dataset_id})

        # Soft-delete isn't typical for config tables, but mark updated
        source_type = existing.get("source_type")
        if source_type in self._SOURCE_CONFIG_TABLES:
            config_table = self._SOURCE_CONFIG_TABLES[source_type]
            config_sql = (
                f"UPDATE {self._table(config_table)} "
                f"SET updated_at = :updated_at, updated_by = :updated_by "
                f"WHERE dataset_id = :dataset_id"
            )
            self._conn.execute(config_sql, {**audit, "dataset_id": dataset_id})


class ConnectionRepository(BaseRepository):
    """CRUD operations for etl_connection."""

    def create(self, data: dict, user: str = "system") -> dict:
        """Create a new connection."""
        fields = {**data, **self._audit_fields(user)}
        fields.setdefault("is_deleted", False)
        cols = ", ".join(fields.keys())
        vals = ", ".join(f":{k}" for k in fields.keys())
        sql = (
            f"INSERT INTO {self._table('etl_connection')} ({cols}) "
            f"VALUES ({vals}) RETURNING *"
        )
        if self._conn.dialect.dialect_type.value == "sqlserver":
            sql = (
                f"INSERT INTO {self._table('etl_connection')} ({cols}) "
                f"OUTPUT INSERTED.* VALUES ({vals})"
            )
        rows = self._conn.execute(sql, fields)
        return rows[0] if rows else {}

    def get(self, connection_id: int) -> dict:
        """Get a connection by ID."""
        sql = (
            f"SELECT * FROM {self._table('etl_connection')} "
            f"WHERE id = :id AND is_deleted = :is_deleted"
        )
        rows = self._conn.execute(sql, {"id": connection_id, "is_deleted": False})
        if not rows:
            raise NotFoundError("Connection", connection_id)
        return rows[0]

    def list(
        self,
        connection_type: str | None = None,
        environment: str | None = None,
    ) -> list[dict]:
        """List connections, optionally filtered by type and environment."""
        sql = f"SELECT * FROM {self._table('etl_connection')} WHERE is_deleted = :is_deleted"
        params: dict[str, Any] = {"is_deleted": False}
        if connection_type:
            sql += " AND connection_type = :connection_type"
            params["connection_type"] = connection_type
        if environment:
            sql += " AND environment = :environment"
            params["environment"] = environment
        sql += " ORDER BY connection_name, id"
        return self._conn.execute(sql, params)

    def update(self, connection_id: int, data: dict, user: str = "system") -> dict:
        """Update a connection."""
        self.get(connection_id)
        fields = {**data, **self._update_audit(user)}
        set_clause = ", ".join(f"{k} = :{k}" for k in fields.keys())
        fields["id"] = connection_id
        sql = (
            f"UPDATE {self._table('etl_connection')} "
            f"SET {set_clause} WHERE id = :id"
        )
        self._conn.execute(sql, fields)
        return self.get(connection_id)

    def delete(self, connection_id: int, user: str = "system") -> None:
        """Soft delete a connection."""
        self.get(connection_id)
        audit = self._update_audit(user)
        sql = (
            f"UPDATE {self._table('etl_connection')} "
            f"SET is_deleted = :is_deleted, updated_at = :updated_at, updated_by = :updated_by "
            f"WHERE id = :id"
        )
        self._conn.execute(sql, {"is_deleted": True, **audit, "id": connection_id})


class HookRepository(BaseRepository):
    """CRUD operations for etl_hook."""

    def create(self, data: dict, user: str = "system") -> dict:
        """Create a new hook."""
        fields = {**data, **self._audit_fields(user)}
        fields.setdefault("is_enabled", True)
        fields.setdefault("is_deleted", False)
        cols = ", ".join(fields.keys())
        vals = ", ".join(f":{k}" for k in fields.keys())
        sql = (
            f"INSERT INTO {self._table('etl_hook')} ({cols}) "
            f"VALUES ({vals}) RETURNING *"
        )
        if self._conn.dialect.dialect_type.value == "sqlserver":
            sql = (
                f"INSERT INTO {self._table('etl_hook')} ({cols}) "
                f"OUTPUT INSERTED.* VALUES ({vals})"
            )
        rows = self._conn.execute(sql, fields)
        return rows[0] if rows else {}

    def list(
        self,
        entity_type: str,
        entity_id: int,
        include_disabled: bool = False,
    ) -> list[dict]:
        """List hooks for an entity, ordered by execution_order."""
        sql = (
            f"SELECT * FROM {self._table('etl_hook')} "
            f"WHERE entity_type = :entity_type AND entity_id = :entity_id "
            f"AND is_deleted = :is_deleted"
        )
        params: dict[str, Any] = {
            "entity_type": entity_type,
            "entity_id": entity_id,
            "is_deleted": False,
        }
        if not include_disabled:
            sql += " AND is_enabled = :is_enabled"
            params["is_enabled"] = True
        sql += " ORDER BY execution_order, id"
        return self._conn.execute(sql, params)

    def update(self, hook_id: int, data: dict, user: str = "system") -> dict:
        """Update a hook."""
        fields = {**data, **self._update_audit(user)}
        set_clause = ", ".join(f"{k} = :{k}" for k in fields.keys())
        fields["id"] = hook_id
        sql = (
            f"UPDATE {self._table('etl_hook')} "
            f"SET {set_clause} WHERE id = :id"
        )
        self._conn.execute(sql, fields)
        sql = f"SELECT * FROM {self._table('etl_hook')} WHERE id = :id"
        rows = self._conn.execute(sql, {"id": hook_id})
        return rows[0] if rows else {}

    def delete(self, hook_id: int, user: str = "system") -> None:
        """Soft delete a hook."""
        audit = self._update_audit(user)
        sql = (
            f"UPDATE {self._table('etl_hook')} "
            f"SET is_deleted = :is_deleted, updated_at = :updated_at, updated_by = :updated_by "
            f"WHERE id = :id"
        )
        self._conn.execute(sql, {"is_deleted": True, **audit, "id": hook_id})


class TagRepository(BaseRepository):
    """CRUD operations for etl_tag."""

    def add(self, data: dict, user: str = "system") -> dict:
        """Add a tag to an entity."""
        fields = {**data, **self._audit_fields(user)}
        cols = ", ".join(fields.keys())
        vals = ", ".join(f":{k}" for k in fields.keys())
        sql = (
            f"INSERT INTO {self._table('etl_tag')} ({cols}) "
            f"VALUES ({vals}) RETURNING *"
        )
        if self._conn.dialect.dialect_type.value == "sqlserver":
            sql = (
                f"INSERT INTO {self._table('etl_tag')} ({cols}) "
                f"OUTPUT INSERTED.* VALUES ({vals})"
            )
        rows = self._conn.execute(sql, fields)
        return rows[0] if rows else {}

    def list(self, entity_type: str, entity_id: int) -> list[dict]:
        """List tags for an entity."""
        sql = (
            f"SELECT * FROM {self._table('etl_tag')} "
            f"WHERE entity_type = :entity_type AND entity_id = :entity_id "
            f"ORDER BY tag_key"
        )
        return self._conn.execute(
            sql, {"entity_type": entity_type, "entity_id": entity_id}
        )

    def remove(self, tag_id: int) -> None:
        """Delete a tag (hard delete)."""
        sql = f"DELETE FROM {self._table('etl_tag')} WHERE id = :id"
        self._conn.execute(sql, {"id": tag_id})


class LineageRepository(BaseRepository):
    """CRUD operations for etl_dataset_lineage."""

    def add(self, data: dict, user: str = "system") -> dict:
        """Add a lineage link."""
        fields = {**data, **self._audit_fields(user)}
        cols = ", ".join(fields.keys())
        vals = ", ".join(f":{k}" for k in fields.keys())
        sql = (
            f"INSERT INTO {self._table('etl_dataset_lineage')} ({cols}) "
            f"VALUES ({vals}) RETURNING *"
        )
        if self._conn.dialect.dialect_type.value == "sqlserver":
            sql = (
                f"INSERT INTO {self._table('etl_dataset_lineage')} ({cols}) "
                f"OUTPUT INSERTED.* VALUES ({vals})"
            )
        rows = self._conn.execute(sql, fields)
        return rows[0] if rows else {}

    def get_upstream(self, dataset_id: int) -> list[dict]:
        """Get datasets that feed into the given dataset."""
        sql = (
            f"SELECT dl.*, d.dataset_name AS source_dataset_name "
            f"FROM {self._table('etl_dataset_lineage')} dl "
            f"JOIN {self._table('etl_dataset')} d ON dl.source_dataset_id = d.id "
            f"WHERE dl.target_dataset_id = :dataset_id"
        )
        return self._conn.execute(sql, {"dataset_id": dataset_id})

    def get_downstream(self, dataset_id: int) -> list[dict]:
        """Get datasets that the given dataset feeds."""
        sql = (
            f"SELECT dl.*, d.dataset_name AS target_dataset_name "
            f"FROM {self._table('etl_dataset_lineage')} dl "
            f"JOIN {self._table('etl_dataset')} d ON dl.target_dataset_id = d.id "
            f"WHERE dl.source_dataset_id = :dataset_id"
        )
        return self._conn.execute(sql, {"dataset_id": dataset_id})


class SystemConfigRepository(BaseRepository):
    """CRUD operations for etl_system_config."""

    def get(self, config_key: str) -> dict:
        """Get a config entry by key."""
        sql = (
            f"SELECT * FROM {self._table('etl_system_config')} "
            f"WHERE config_key = :config_key"
        )
        rows = self._conn.execute(sql, {"config_key": config_key})
        if not rows:
            raise NotFoundError("SystemConfig", 0)
        return rows[0]

    def set(self, config_key: str, config_value: str, user: str = "system") -> dict:
        """Set a config value (upsert)."""
        try:
            existing = self.get(config_key)
            fields = {
                "config_value": config_value,
                **self._update_audit(user),
                "config_key": config_key,
            }
            sql = (
                f"UPDATE {self._table('etl_system_config')} "
                f"SET config_value = :config_value, updated_at = :updated_at, updated_by = :updated_by "
                f"WHERE config_key = :config_key"
            )
            self._conn.execute(sql, fields)
        except NotFoundError:
            fields = {
                "config_key": config_key,
                "config_value": config_value,
                **self._audit_fields(user),
            }
            cols = ", ".join(fields.keys())
            vals = ", ".join(f":{k}" for k in fields.keys())
            sql = (
                f"INSERT INTO {self._table('etl_system_config')} ({cols}) "
                f"VALUES ({vals})"
            )
            self._conn.execute(sql, fields)

        return self.get(config_key)

    def list(self) -> list[dict]:
        """List all config entries."""
        sql = f"SELECT * FROM {self._table('etl_system_config')} ORDER BY config_key"
        return self._conn.execute(sql)


class ExecutionRepository(BaseRepository):
    """Repository that wraps stored procedure calls for execution lifecycle."""

    def start_process_execution(
        self,
        process_id: int,
        environment: str,
        triggered_by: str = "manual",
        parameters: str | None = None,
        user: str = "system",
    ) -> int:
        """Start a process execution via stored procedure."""
        params: dict[str, Any] = {
            "process_id": process_id,
            "environment": environment,
            "triggered_by": triggered_by,
            "updated_by": user,
        }
        if parameters:
            params["parameters"] = parameters

        rows = self._conn.execute_procedure("sp_start_process_execution", params)
        if rows:
            return rows[0].get("process_execution_id", rows[0].get("id", 0))
        return 0

    def complete_process_execution(
        self, process_execution_id: int, user: str = "system"
    ) -> None:
        """Complete a process execution."""
        self._conn.execute_procedure(
            "sp_complete_process_execution",
            {"process_execution_id": process_execution_id, "updated_by": user},
        )

    def start_job_execution(
        self, job_execution_id: int, user: str = "system"
    ) -> None:
        """Start a job execution."""
        self._conn.execute_procedure(
            "sp_start_job_execution",
            {"job_execution_id": job_execution_id, "updated_by": user},
        )

    def complete_job_execution(
        self, job_execution_id: int, user: str = "system"
    ) -> None:
        """Complete a job execution."""
        self._conn.execute_procedure(
            "sp_complete_job_execution",
            {"job_execution_id": job_execution_id, "updated_by": user},
        )

    def start_dataset_execution(
        self, dataset_execution_id: int, user: str = "system"
    ) -> None:
        """Start a dataset execution."""
        self._conn.execute_procedure(
            "sp_start_dataset_execution",
            {"dataset_execution_id": dataset_execution_id, "updated_by": user},
        )

    def complete_dataset_execution(
        self,
        dataset_execution_id: int,
        status: str,
        rows_read: int | None = None,
        rows_written: int | None = None,
        rows_errored: int | None = None,
        bytes_processed: int | None = None,
        error_message: str | None = None,
        user: str = "system",
    ) -> None:
        """Complete a dataset execution."""
        params: dict[str, Any] = {
            "dataset_execution_id": dataset_execution_id,
            "status": status,
            "updated_by": user,
        }
        if rows_read is not None:
            params["rows_read"] = rows_read
        if rows_written is not None:
            params["rows_written"] = rows_written
        if rows_errored is not None:
            params["rows_errored"] = rows_errored
        if bytes_processed is not None:
            params["bytes_processed"] = bytes_processed
        if error_message is not None:
            params["error_message"] = error_message

        self._conn.execute_procedure("sp_complete_dataset_execution", params)

    def get_scheduled_processes(self) -> list[dict]:
        """Get all enabled processes with active schedules."""
        return self._conn.execute_procedure("sp_get_scheduled_processes")

    def get_jobs_to_execute(self, process_execution_id: int) -> list[dict]:
        """Get pending jobs for a process execution."""
        return self._conn.execute_procedure(
            "sp_get_jobs_to_execute",
            {"process_execution_id": process_execution_id},
        )

    def get_datasets_to_execute(self, job_execution_id: int) -> list[dict]:
        """Get pending datasets for a job execution."""
        return self._conn.execute_procedure(
            "sp_get_datasets_to_execute",
            {"job_execution_id": job_execution_id},
        )

    def get_execution_summary(
        self,
        process_id: int | None = None,
        environment: str | None = None,
        status: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> list[dict]:
        """Get execution summary with optional filters."""
        params: dict[str, Any] = {}
        if process_id is not None:
            params["process_id"] = process_id
        if environment is not None:
            params["environment"] = environment
        if status is not None:
            params["status"] = status
        if start_date is not None:
            params["start_date"] = start_date
        if end_date is not None:
            params["end_date"] = end_date

        return self._conn.execute_procedure("sp_get_execution_summary", params)

    def retry_failed_datasets(
        self, job_execution_id: int, user: str = "system"
    ) -> list[dict]:
        """Retry failed datasets for a job execution."""
        return self._conn.execute_procedure(
            "sp_retry_failed_datasets",
            {"job_execution_id": job_execution_id, "updated_by": user},
        )
