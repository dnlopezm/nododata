"""CLI entry point for the Nodo ETL Framework."""

import click

from nodo_etl import __version__


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


if __name__ == "__main__":
    cli()
