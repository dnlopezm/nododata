"""CLI entry point for the Nodo ETL Framework."""

import os

import click

from nodo_etl import __version__
from nodo_etl.cli.connection import connection
from nodo_etl.cli.dataset import dataset
from nodo_etl.cli.execution import history, retry, run, status
from nodo_etl.cli.job import job
from nodo_etl.cli.process import process
from nodo_etl.cli.utility import config, hook, lineage, tag


def _get_connection(ctx: click.Context):
    """Get or create the database connection from context."""
    if "connection" not in ctx.obj:
        from nodo_etl.config.settings import Settings
        from nodo_etl.db.connection import MetadataDBConnection

        env_overrides = {}
        if ctx.obj.get("db_type"):
            env_overrides["NODO_ETL_DB_TYPE"] = ctx.obj["db_type"]
        if ctx.obj.get("env"):
            env_overrides["NODO_ETL_ENVIRONMENT"] = ctx.obj["env"]

        for key, val in env_overrides.items():
            os.environ[key] = val

        settings = Settings()
        ctx.obj["connection"] = MetadataDBConnection(settings)
        ctx.obj["settings"] = settings

    return ctx.obj["connection"]


@click.group()
@click.version_option(version=__version__, prog_name="nodo-etl")
@click.option(
    "--env",
    type=click.Choice(["dev", "staging", "prod"]),
    default="dev",
    help="Target environment.",
)
@click.option(
    "--db-type",
    type=click.Choice(["sqlserver", "postgresql"]),
    default=None,
    help="Metadata database type (overrides config).",
)
@click.pass_context
def cli(ctx: click.Context, env: str, db_type: str | None) -> None:
    """Nodo ETL Framework - Metadata-driven pipeline orchestration."""
    ctx.ensure_object(dict)
    ctx.obj["env"] = env
    ctx.obj["db_type"] = db_type
    ctx.obj["get_connection"] = lambda: _get_connection(ctx)


# Register command groups
cli.add_command(connection)
cli.add_command(process)
cli.add_command(job)
cli.add_command(dataset)
cli.add_command(run)
cli.add_command(status)
cli.add_command(history)
cli.add_command(retry)
cli.add_command(config)
cli.add_command(tag)
cli.add_command(hook)
cli.add_command(lineage)


if __name__ == "__main__":
    cli()
