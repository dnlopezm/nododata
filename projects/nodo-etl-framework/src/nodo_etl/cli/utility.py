"""CLI utility commands for config, tags, hooks, and lineage."""

import click

from nodo_etl.cli.output import print_error, print_success, print_table
from nodo_etl.db.repositories import (
    HookRepository,
    LineageRepository,
    NotFoundError,
    SystemConfigRepository,
    TagRepository,
)


# -- Config Commands --

@click.group()
@click.pass_context
def config(ctx: click.Context) -> None:
    """Manage system configuration."""
    pass


@config.command("list")
@click.pass_context
def config_list(ctx):
    """Show all system config values."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = SystemConfigRepository(conn)
        rows = repo.list()
        if not rows:
            click.echo("No config entries found.")
            return
        print_table("System Config", ["config_key", "config_value", "description"], rows)
    except Exception as e:
        print_error(str(e))


@config.command("set")
@click.argument("key")
@click.argument("value")
@click.pass_context
def config_set(ctx, key, value):
    """Set a system config value."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = SystemConfigRepository(conn)
        repo.set(key, value)
        print_success(f"Config '{key}' set to '{value}'.")
    except Exception as e:
        print_error(str(e))


# -- Tag Commands --

@click.group()
@click.pass_context
def tag(ctx: click.Context) -> None:
    """Manage entity tags."""
    pass


@tag.command("add")
@click.argument("entity_type", type=click.Choice(["process", "job", "dataset"]))
@click.argument("entity_id", type=int)
@click.argument("key")
@click.argument("value")
@click.pass_context
def tag_add(ctx, entity_type, entity_id, key, value):
    """Add a tag to an entity."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = TagRepository(conn)
        result = repo.add({
            "entity_type": entity_type,
            "entity_id": entity_id,
            "tag_key": key,
            "tag_value": value,
        })
        print_success(f"Tag '{key}={value}' added (id={result.get('id')}).")
    except Exception as e:
        print_error(str(e))


@tag.command("list")
@click.argument("entity_type", type=click.Choice(["process", "job", "dataset"]))
@click.argument("entity_id", type=int)
@click.pass_context
def tag_list(ctx, entity_type, entity_id):
    """List tags for an entity."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = TagRepository(conn)
        rows = repo.list(entity_type, entity_id)
        if not rows:
            click.echo("No tags found.")
            return
        print_table("Tags", ["id", "tag_key", "tag_value"], rows)
    except Exception as e:
        print_error(str(e))


# -- Hook Commands --

@click.group()
@click.pass_context
def hook(ctx: click.Context) -> None:
    """Manage pre/post hooks."""
    pass


@hook.command("add")
@click.option("--entity-type", required=True, type=click.Choice(["process", "job", "dataset"]))
@click.option("--entity-id", required=True, type=int)
@click.option("--hook-type", required=True, type=click.Choice(["pre", "post"]))
@click.option("--action-type", required=True, type=click.Choice(["sql", "api", "command"]))
@click.option("--action-config", required=True, help="Action config as JSON string.")
@click.option("--order", default=1, type=int, help="Execution order.")
@click.option("--on-status", default=None, help="Trigger on status (post hooks only).")
@click.pass_context
def hook_add(ctx, entity_type, entity_id, hook_type, action_type, action_config, order, on_status):
    """Add a pre/post hook."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = HookRepository(conn)
        data = {
            "entity_type": entity_type,
            "entity_id": entity_id,
            "hook_type": hook_type,
            "action_type": action_type,
            "action_config": action_config,
            "execution_order": order,
        }
        if on_status:
            data["on_status"] = on_status
        result = repo.create(data)
        print_success(f"Hook created with id={result.get('id')}.")
    except Exception as e:
        print_error(str(e))


@hook.command("list")
@click.argument("entity_type", type=click.Choice(["process", "job", "dataset"]))
@click.argument("entity_id", type=int)
@click.pass_context
def hook_list(ctx, entity_type, entity_id):
    """List hooks for an entity."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = HookRepository(conn)
        rows = repo.list(entity_type, entity_id, include_disabled=True)
        if not rows:
            click.echo("No hooks found.")
            return
        print_table(
            "Hooks",
            ["id", "hook_type", "action_type", "execution_order", "is_enabled"],
            rows,
        )
    except Exception as e:
        print_error(str(e))


# -- Lineage Commands --

@click.group()
@click.pass_context
def lineage(ctx: click.Context) -> None:
    """Manage dataset lineage."""
    pass


@lineage.command("add")
@click.argument("source_id", type=int)
@click.argument("target_id", type=int)
@click.pass_context
def lineage_add(ctx, source_id, target_id):
    """Add a lineage link between datasets."""
    if source_id == target_id:
        print_error("Cannot create self-referencing lineage.")
        return
    try:
        conn = ctx.obj["get_connection"]()
        repo = LineageRepository(conn)
        result = repo.add({
            "source_dataset_id": source_id,
            "target_dataset_id": target_id,
        })
        print_success(f"Lineage link created (id={result.get('id')}).")
    except Exception as e:
        print_error(str(e))


@lineage.command("show")
@click.argument("dataset_id", type=int)
@click.pass_context
def lineage_show(ctx, dataset_id):
    """Show upstream and downstream lineage for a dataset."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = LineageRepository(conn)

        upstream = repo.get_upstream(dataset_id)
        downstream = repo.get_downstream(dataset_id)

        if upstream:
            click.echo("\nUpstream (feeds into this dataset):")
            for u in upstream:
                click.echo(f"  <- {u.get('source_dataset_name')} (id={u.get('source_dataset_id')})")
        else:
            click.echo("\nNo upstream datasets.")

        if downstream:
            click.echo("\nDownstream (this dataset feeds):")
            for d in downstream:
                click.echo(f"  -> {d.get('target_dataset_name')} (id={d.get('target_dataset_id')})")
        else:
            click.echo("\nNo downstream datasets.")
    except Exception as e:
        print_error(str(e))
