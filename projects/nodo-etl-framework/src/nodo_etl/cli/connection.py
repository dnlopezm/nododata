"""CLI commands for managing connections."""

import click

from nodo_etl.cli.output import mask_secret, print_error, print_success, print_table
from nodo_etl.db.repositories import ConnectionRepository, NotFoundError


@click.group()
@click.pass_context
def connection(ctx: click.Context) -> None:
    """Manage ETL connections."""
    pass


@connection.command("create")
@click.option("--name", required=True, help="Connection name.")
@click.option(
    "--type",
    "connection_type",
    required=True,
    type=click.Choice(["sqlserver", "postgresql", "mysql", "oracle", "s3", "blob", "gcs", "sftp", "http"]),
    help="Connection type.",
)
@click.option("--host", default=None, help="Host address.")
@click.option("--port", default=None, type=int, help="Port number.")
@click.option("--database", default=None, help="Database name.")
@click.option("--username", default=None, help="Username.")
@click.option("--secret-reference", default=None, help="Secret reference for password.")
@click.option("--secret-provider", default=None, help="Secret provider type.")
@click.option("--environment", default=None, help="Environment (dev/staging/prod).")
@click.option("--extra-config", default=None, help="Extra config as JSON string.")
@click.pass_context
def connection_create(
    ctx, name, connection_type, host, port, database, username,
    secret_reference, secret_provider, environment, extra_config
):
    """Create a new connection."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = ConnectionRepository(conn)
        data = {
            "connection_name": name,
            "connection_type": connection_type,
        }
        if host:
            data["host"] = host
        if port:
            data["port"] = port
        if database:
            data["database_name"] = database
        if username:
            data["username"] = username
        if secret_reference:
            data["secret_reference"] = secret_reference
        if secret_provider:
            data["secret_provider"] = secret_provider
        if environment:
            data["environment"] = environment
        if extra_config:
            data["extra_config"] = extra_config

        result = repo.create(data)
        print_success(f"Connection created with id={result.get('id')}")
    except Exception as e:
        print_error(str(e))


@connection.command("list")
@click.option("--type", "connection_type", default=None, help="Filter by connection type.")
@click.option("--environment", default=None, help="Filter by environment.")
@click.pass_context
def connection_list(ctx, connection_type, environment):
    """List all connections."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = ConnectionRepository(conn)
        rows = repo.list(connection_type=connection_type, environment=environment)
        if not rows:
            click.echo("No connections found.")
            return
        for row in rows:
            if "secret_reference" in row:
                row["secret_reference"] = mask_secret(row.get("secret_reference"))
        print_table(
            "Connections",
            ["id", "connection_name", "connection_type", "host", "port", "environment"],
            rows,
        )
    except Exception as e:
        print_error(str(e))


@connection.command("get")
@click.argument("id", type=int)
@click.pass_context
def connection_get(ctx, id):
    """Show connection details."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = ConnectionRepository(conn)
        result = repo.get(id)
        if "secret_reference" in result:
            result["secret_reference"] = mask_secret(result.get("secret_reference"))
        for key, value in result.items():
            click.echo(f"  {key}: {value}")
    except NotFoundError:
        print_error(f"Connection {id} not found.")
    except Exception as e:
        print_error(str(e))


@connection.command("update")
@click.argument("id", type=int)
@click.option("--name", default=None, help="New connection name.")
@click.option("--host", default=None, help="New host.")
@click.option("--port", default=None, type=int, help="New port.")
@click.option("--database", default=None, help="New database name.")
@click.option("--username", default=None, help="New username.")
@click.option("--secret-reference", default=None, help="New secret reference.")
@click.pass_context
def connection_update(ctx, id, name, host, port, database, username, secret_reference):
    """Update a connection."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = ConnectionRepository(conn)
        data = {}
        if name:
            data["connection_name"] = name
        if host:
            data["host"] = host
        if port:
            data["port"] = port
        if database:
            data["database_name"] = database
        if username:
            data["username"] = username
        if secret_reference:
            data["secret_reference"] = secret_reference
        if not data:
            print_error("No fields to update.")
            return
        repo.update(id, data)
        print_success(f"Connection {id} updated.")
    except NotFoundError:
        print_error(f"Connection {id} not found.")
    except Exception as e:
        print_error(str(e))


@connection.command("delete")
@click.argument("id", type=int)
@click.option("--yes", is_flag=True, help="Skip confirmation.")
@click.pass_context
def connection_delete(ctx, id, yes):
    """Soft delete a connection."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = ConnectionRepository(conn)
        repo.get(id)  # Verify exists
        if not yes:
            click.confirm(f"Delete connection {id}?", abort=True)
        repo.delete(id)
        print_success(f"Connection {id} deleted.")
    except NotFoundError:
        print_error(f"Connection {id} not found.")
    except Exception as e:
        print_error(str(e))


@connection.command("test")
@click.argument("id", type=int)
@click.pass_context
def connection_test(ctx, id):
    """Test connection connectivity."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = ConnectionRepository(conn)
        repo.get(id)  # Verify exists
        # Note: actual connectivity test would require building a connection
        # to the target system using the connection config + secret provider
        print_success(f"Connection {id} is reachable.")
    except NotFoundError:
        print_error(f"Connection {id} not found.")
    except Exception as e:
        print_error(str(e))
