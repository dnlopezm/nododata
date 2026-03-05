"""CLI commands for managing jobs."""

import click

from nodo_etl.cli.output import print_error, print_success, print_table
from nodo_etl.db.repositories import DatasetRepository, JobRepository, NotFoundError


@click.group()
@click.pass_context
def job(ctx: click.Context) -> None:
    """Manage ETL jobs."""
    pass


@job.command("create")
@click.option("--process-id", required=True, type=int, help="Parent process ID.")
@click.option("--name", required=True, help="Job name.")
@click.option("--description", default=None, help="Job description.")
@click.option("--order", default=1, type=int, help="Execution order.")
@click.option("--max-parallelism", default=None, type=int, help="Max parallel datasets.")
@click.option("--max-retries", default=None, type=int, help="Max retries.")
@click.pass_context
def job_create(ctx, process_id, name, description, order, max_parallelism, max_retries):
    """Create a new job under a process."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = JobRepository(conn)
        data = {
            "process_id": process_id,
            "job_name": name,
            "execution_order": order,
        }
        if description:
            data["description"] = description
        if max_parallelism is not None:
            data["max_parallelism"] = max_parallelism
        if max_retries is not None:
            data["max_retries"] = max_retries
        result = repo.create(data)
        print_success(f"Job created with id={result.get('id')}")
    except Exception as e:
        print_error(str(e))


@job.command("list")
@click.option("--process-id", required=True, type=int, help="Process ID.")
@click.pass_context
def job_list(ctx, process_id):
    """List jobs for a process."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = JobRepository(conn)
        rows = repo.list(process_id)
        if not rows:
            click.echo("No jobs found.")
            return
        print_table(
            "Jobs",
            ["id", "job_name", "execution_order", "max_parallelism", "is_enabled"],
            rows,
        )
    except Exception as e:
        print_error(str(e))


@job.command("get")
@click.argument("id", type=int)
@click.pass_context
def job_get(ctx, id):
    """Show job details with datasets."""
    try:
        conn = ctx.obj["get_connection"]()
        job_repo = JobRepository(conn)
        result = job_repo.get(id)
        click.echo(f"\nJob: {result.get('job_name')}")
        for key, value in result.items():
            click.echo(f"  {key}: {value}")

        ds_repo = DatasetRepository(conn)
        datasets = ds_repo.list(id)
        if datasets:
            click.echo(f"\nDatasets ({len(datasets)}):")
            for d in datasets:
                click.echo(
                    f"  [{d.get('execution_order')}] {d.get('dataset_name')} "
                    f"({d.get('source_type')}/{d.get('layer')}/{d.get('load_strategy')})"
                )
    except NotFoundError:
        print_error(f"Job {id} not found.")
    except Exception as e:
        print_error(str(e))


@job.command("update")
@click.argument("id", type=int)
@click.option("--name", default=None, help="New job name.")
@click.option("--description", default=None, help="New description.")
@click.option("--order", default=None, type=int, help="New execution order.")
@click.option("--max-parallelism", default=None, type=int, help="New max parallelism.")
@click.option("--max-retries", default=None, type=int, help="New max retries.")
@click.pass_context
def job_update(ctx, id, name, description, order, max_parallelism, max_retries):
    """Update a job."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = JobRepository(conn)
        data = {}
        if name:
            data["job_name"] = name
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
        print_success(f"Job {id} updated.")
    except NotFoundError:
        print_error(f"Job {id} not found.")
    except Exception as e:
        print_error(str(e))


@job.command("delete")
@click.argument("id", type=int)
@click.option("--yes", is_flag=True, help="Skip confirmation.")
@click.pass_context
def job_delete(ctx, id, yes):
    """Soft delete a job (cascades to datasets)."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = JobRepository(conn)
        repo.get(id)
        if not yes:
            click.confirm(f"Delete job {id}? This cascades to datasets.", abort=True)
        repo.delete(id)
        print_success(f"Job {id} deleted.")
    except NotFoundError:
        print_error(f"Job {id} not found.")
    except Exception as e:
        print_error(str(e))


@job.command("enable")
@click.argument("id", type=int)
@click.pass_context
def job_enable(ctx, id):
    """Enable a job."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = JobRepository(conn)
        repo.update(id, {"is_enabled": True})
        print_success(f"Job {id} enabled.")
    except NotFoundError:
        print_error(f"Job {id} not found.")
    except Exception as e:
        print_error(str(e))


@job.command("disable")
@click.argument("id", type=int)
@click.pass_context
def job_disable(ctx, id):
    """Disable a job."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = JobRepository(conn)
        repo.update(id, {"is_enabled": False})
        print_success(f"Job {id} disabled.")
    except NotFoundError:
        print_error(f"Job {id} not found.")
    except Exception as e:
        print_error(str(e))
