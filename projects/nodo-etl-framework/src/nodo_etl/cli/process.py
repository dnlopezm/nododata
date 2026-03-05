"""CLI commands for managing processes."""

import click

from nodo_etl.cli.output import print_error, print_success, print_table
from nodo_etl.db.repositories import NotFoundError, ProcessRepository


@click.group()
@click.pass_context
def process(ctx: click.Context) -> None:
    """Manage ETL processes."""
    pass


@process.command("create")
@click.option("--name", required=True, help="Process name.")
@click.option("--description", default=None, help="Process description.")
@click.option("--order", default=1, type=int, help="Execution order.")
@click.option("--max-parallelism", default=None, type=int, help="Max parallel jobs.")
@click.option("--max-retries", default=None, type=int, help="Max retries.")
@click.option("--schedule-name", default=None, help="Schedule name.")
@click.option("--cron", default=None, help="Cron expression for schedule.")
@click.pass_context
def process_create(ctx, name, description, order, max_parallelism, max_retries, schedule_name, cron):
    """Create a new process."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = ProcessRepository(conn)
        data = {"process_name": name, "execution_order": order}
        if description:
            data["description"] = description
        if max_parallelism is not None:
            data["max_parallelism"] = max_parallelism
        if max_retries is not None:
            data["max_retries"] = max_retries
        result = repo.create(data)
        process_id = result.get("id")
        print_success(f"Process created with id={process_id}")

        if schedule_name and cron:
            schedule = repo.create_schedule({
                "process_id": process_id,
                "schedule_name": schedule_name,
                "cron_expression": cron,
            })
            print_success(f"Schedule created with id={schedule.get('id')}")
    except Exception as e:
        print_error(str(e))


@process.command("list")
@click.option("--enabled/--disabled", default=None, help="Filter by enabled status.")
@click.pass_context
def process_list(ctx, enabled):
    """List all processes."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = ProcessRepository(conn)
        rows = repo.list(is_enabled=enabled)
        if not rows:
            click.echo("No processes found.")
            return
        print_table(
            "Processes",
            ["id", "process_name", "execution_order", "max_parallelism", "is_enabled"],
            rows,
        )
    except Exception as e:
        print_error(str(e))


@process.command("get")
@click.argument("id", type=int)
@click.pass_context
def process_get(ctx, id):
    """Show process details with jobs and schedules."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = ProcessRepository(conn)
        result = repo.get(id)
        click.echo(f"\nProcess: {result.get('process_name')}")
        for key, value in result.items():
            click.echo(f"  {key}: {value}")

        schedules = repo.list_schedules(id)
        if schedules:
            click.echo(f"\nSchedules ({len(schedules)}):")
            for s in schedules:
                click.echo(f"  - {s.get('schedule_name')}: {s.get('cron_expression')}")

        from nodo_etl.db.repositories import JobRepository, DatasetRepository
        job_repo = JobRepository(conn)
        jobs = job_repo.list(id)
        if jobs:
            click.echo(f"\nJobs ({len(jobs)}):")
            ds_repo = DatasetRepository(conn)
            for j in jobs:
                click.echo(f"  [{j.get('execution_order')}] {j.get('job_name')} (id={j.get('id')})")
                datasets = ds_repo.list(j.get("id"))
                for d in datasets:
                    click.echo(f"      [{d.get('execution_order')}] {d.get('dataset_name')} ({d.get('source_type')}/{d.get('layer')})")
    except NotFoundError:
        print_error(f"Process {id} not found.")
    except Exception as e:
        print_error(str(e))


@process.command("update")
@click.argument("id", type=int)
@click.option("--name", default=None, help="New process name.")
@click.option("--description", default=None, help="New description.")
@click.option("--order", default=None, type=int, help="New execution order.")
@click.option("--max-parallelism", default=None, type=int, help="New max parallelism.")
@click.option("--max-retries", default=None, type=int, help="New max retries.")
@click.pass_context
def process_update(ctx, id, name, description, order, max_parallelism, max_retries):
    """Update a process."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = ProcessRepository(conn)
        data = {}
        if name:
            data["process_name"] = name
        if description:
            data["description"] = description
        if order is not None:
            data["execution_order"] = order
        if max_parallelism is not None:
            data["max_parallelism"] = max_parallelism
        if max_retries is not None:
            data["max_retries"] = max_retries
        if not data:
            print_error("No fields to update.")
            return
        repo.update(id, data)
        print_success(f"Process {id} updated.")
    except NotFoundError:
        print_error(f"Process {id} not found.")
    except Exception as e:
        print_error(str(e))


@process.command("delete")
@click.argument("id", type=int)
@click.option("--yes", is_flag=True, help="Skip confirmation.")
@click.pass_context
def process_delete(ctx, id, yes):
    """Soft delete a process (cascades to jobs and datasets)."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = ProcessRepository(conn)
        repo.get(id)
        if not yes:
            click.confirm(f"Delete process {id}? This cascades to jobs and datasets.", abort=True)
        repo.delete(id)
        print_success(f"Process {id} deleted.")
    except NotFoundError:
        print_error(f"Process {id} not found.")
    except Exception as e:
        print_error(str(e))


@process.command("enable")
@click.argument("id", type=int)
@click.pass_context
def process_enable(ctx, id):
    """Enable a process."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = ProcessRepository(conn)
        repo.update(id, {"is_enabled": True})
        print_success(f"Process {id} enabled.")
    except NotFoundError:
        print_error(f"Process {id} not found.")
    except Exception as e:
        print_error(str(e))


@process.command("disable")
@click.argument("id", type=int)
@click.pass_context
def process_disable(ctx, id):
    """Disable a process."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = ProcessRepository(conn)
        repo.update(id, {"is_enabled": False})
        print_success(f"Process {id} disabled.")
    except NotFoundError:
        print_error(f"Process {id} not found.")
    except Exception as e:
        print_error(str(e))
