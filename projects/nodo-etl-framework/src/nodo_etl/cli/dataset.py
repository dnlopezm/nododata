"""CLI commands for managing datasets."""

import click

from nodo_etl.cli.output import print_error, print_success, print_table
from nodo_etl.db.repositories import DatasetRepository, NotFoundError


@click.group()
@click.pass_context
def dataset(ctx: click.Context) -> None:
    """Manage ETL datasets."""
    pass


@dataset.command("create")
@click.option("--job-id", required=True, type=int, help="Parent job ID.")
@click.option("--name", required=True, help="Dataset name.")
@click.option(
    "--source-type",
    required=True,
    type=click.Choice(["database", "file", "api", "stream"]),
    help="Source type.",
)
@click.option(
    "--layer",
    required=True,
    type=click.Choice(["bronze", "silver", "gold"]),
    help="Medallion layer.",
)
@click.option(
    "--load-strategy",
    default="full",
    type=click.Choice(["full", "incremental", "cdc", "streaming"]),
    help="Load strategy.",
)
@click.option("--order", default=1, type=int, help="Execution order.")
@click.option("--connection-id", default=None, type=int, help="Connection ID.")
@click.option("--max-retries", default=None, type=int, help="Max retries.")
# DB config options
@click.option("--source-schema", default=None, help="Source schema (database).")
@click.option("--source-table", default=None, help="Source table (database).")
@click.option("--source-query", default=None, help="Source query (database).")
@click.option("--target-schema", default=None, help="Target schema (database).")
@click.option("--target-table", default=None, help="Target table (database).")
@click.option("--watermark-column", default=None, help="Watermark column (incremental).")
# File config options
@click.option("--file-path", default=None, help="File path (file source).")
@click.option("--file-format", default=None, help="File format (csv/parquet/json/avro/orc).")
# API config options
@click.option("--api-url", default=None, help="API URL (api source).")
@click.option("--api-method", default=None, help="HTTP method (api source).")
@click.pass_context
def dataset_create(
    ctx, job_id, name, source_type, layer, load_strategy, order, connection_id,
    max_retries, source_schema, source_table, source_query, target_schema,
    target_table, watermark_column, file_path, file_format, api_url, api_method
):
    """Create a new dataset with source config."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = DatasetRepository(conn)
        data = {
            "job_id": job_id,
            "dataset_name": name,
            "source_type": source_type,
            "layer": layer,
            "load_strategy": load_strategy,
            "execution_order": order,
        }
        if connection_id is not None:
            data["connection_id"] = connection_id
        if max_retries is not None:
            data["max_retries"] = max_retries

        source_config = None
        if source_type == "database":
            source_config = {}
            if source_schema:
                source_config["source_schema"] = source_schema
            if source_table:
                source_config["source_table"] = source_table
            if source_query:
                source_config["source_query"] = source_query
            if target_schema:
                source_config["target_schema"] = target_schema
            if target_table:
                source_config["target_table"] = target_table
            if watermark_column:
                source_config["watermark_column"] = watermark_column
            if not source_config:
                source_config = None
        elif source_type == "file":
            source_config = {}
            if file_path:
                source_config["file_path"] = file_path
            if file_format:
                source_config["file_format"] = file_format
            if not source_config:
                source_config = None
        elif source_type == "api":
            source_config = {}
            if api_url:
                source_config["api_url"] = api_url
            if api_method:
                source_config["http_method"] = api_method
            if not source_config:
                source_config = None

        result = repo.create(data, source_config=source_config)
        print_success(f"Dataset created with id={result.get('id')}")
    except Exception as e:
        print_error(str(e))


@dataset.command("list")
@click.option("--job-id", required=True, type=int, help="Job ID.")
@click.pass_context
def dataset_list(ctx, job_id):
    """List datasets for a job."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = DatasetRepository(conn)
        rows = repo.list(job_id)
        if not rows:
            click.echo("No datasets found.")
            return
        print_table(
            "Datasets",
            ["id", "dataset_name", "source_type", "layer", "load_strategy", "execution_order", "is_enabled"],
            rows,
        )
    except Exception as e:
        print_error(str(e))


@dataset.command("get")
@click.argument("id", type=int)
@click.pass_context
def dataset_get(ctx, id):
    """Show dataset details with source config."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = DatasetRepository(conn)
        result = repo.get(id)
        click.echo(f"\nDataset: {result.get('dataset_name')}")
        source_config = result.pop("source_config", None)
        for key, value in result.items():
            click.echo(f"  {key}: {value}")
        if source_config:
            click.echo("\nSource Config:")
            for key, value in source_config.items():
                click.echo(f"  {key}: {value}")
    except NotFoundError:
        print_error(f"Dataset {id} not found.")
    except Exception as e:
        print_error(str(e))


@dataset.command("update")
@click.argument("id", type=int)
@click.option("--name", default=None, help="New dataset name.")
@click.option("--load-strategy", default=None, help="New load strategy.")
@click.option("--order", default=None, type=int, help="New execution order.")
@click.option("--max-retries", default=None, type=int, help="New max retries.")
@click.pass_context
def dataset_update(ctx, id, name, load_strategy, order, max_retries):
    """Update a dataset."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = DatasetRepository(conn)
        data = {}
        if name:
            data["dataset_name"] = name
        if load_strategy:
            data["load_strategy"] = load_strategy
        if order is not None:
            data["execution_order"] = order
        if max_retries is not None:
            data["max_retries"] = max_retries
        if not data:
            print_error("No fields to update.")
            return
        repo.update(id, data)
        print_success(f"Dataset {id} updated.")
    except NotFoundError:
        print_error(f"Dataset {id} not found.")
    except Exception as e:
        print_error(str(e))


@dataset.command("delete")
@click.argument("id", type=int)
@click.option("--yes", is_flag=True, help="Skip confirmation.")
@click.pass_context
def dataset_delete(ctx, id, yes):
    """Soft delete a dataset."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = DatasetRepository(conn)
        repo.get(id)
        if not yes:
            click.confirm(f"Delete dataset {id}?", abort=True)
        repo.delete(id)
        print_success(f"Dataset {id} deleted.")
    except NotFoundError:
        print_error(f"Dataset {id} not found.")
    except Exception as e:
        print_error(str(e))


@dataset.command("enable")
@click.argument("id", type=int)
@click.pass_context
def dataset_enable(ctx, id):
    """Enable a dataset."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = DatasetRepository(conn)
        repo.update(id, {"is_enabled": True})
        print_success(f"Dataset {id} enabled.")
    except NotFoundError:
        print_error(f"Dataset {id} not found.")
    except Exception as e:
        print_error(str(e))


@dataset.command("disable")
@click.argument("id", type=int)
@click.pass_context
def dataset_disable(ctx, id):
    """Disable a dataset."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = DatasetRepository(conn)
        repo.update(id, {"is_enabled": False})
        print_success(f"Dataset {id} disabled.")
    except NotFoundError:
        print_error(f"Dataset {id} not found.")
    except Exception as e:
        print_error(str(e))
