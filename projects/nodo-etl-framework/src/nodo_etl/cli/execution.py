"""CLI commands for execution lifecycle."""

import click

from nodo_etl.cli.output import print_error, print_success, print_table
from nodo_etl.db.repositories import ExecutionRepository, NotFoundError


@click.group()
@click.pass_context
def run(ctx: click.Context) -> None:
    """Run ETL processes, jobs, or datasets."""
    pass


@run.command("process")
@click.argument("id", type=int)
@click.option("--triggered-by", default="manual", help="Trigger source.")
@click.option("--parameters", default=None, help="JSON parameters.")
@click.pass_context
def run_process(ctx, id, triggered_by, parameters):
    """Trigger a full process execution."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = ExecutionRepository(conn)
        env = ctx.obj.get("env", "dev")
        exec_id = repo.start_process_execution(
            process_id=id,
            environment=env,
            triggered_by=triggered_by,
            parameters=parameters,
        )
        print_success(f"Process execution started with id={exec_id}")
    except NotFoundError:
        print_error(f"Process {id} not found.")
    except Exception as e:
        print_error(str(e))


@run.command("job")
@click.argument("id", type=int)
@click.pass_context
def run_job(ctx, id):
    """Start a job execution."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = ExecutionRepository(conn)
        repo.start_job_execution(id)
        print_success(f"Job execution {id} started.")
    except Exception as e:
        print_error(str(e))


@run.command("dataset")
@click.argument("id", type=int)
@click.pass_context
def run_dataset(ctx, id):
    """Start a dataset execution."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = ExecutionRepository(conn)
        repo.start_dataset_execution(id)
        print_success(f"Dataset execution {id} started.")
    except Exception as e:
        print_error(str(e))


@click.command()
@click.argument("process_execution_id", type=int)
@click.pass_context
def status(ctx, process_execution_id):
    """Show execution summary for a process execution."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = ExecutionRepository(conn)
        rows = repo.get_execution_summary(process_id=None)
        # Filter to the specific execution
        filtered = [r for r in rows if r.get("process_execution_id") == process_execution_id]
        if not filtered:
            print_error(f"Execution {process_execution_id} not found.")
            return
        print_table(
            f"Execution Summary (id={process_execution_id})",
            [
                "process_name", "process_status",
                "job_name", "job_status",
                "dataset_name", "dataset_status",
                "dataset_rows_read", "dataset_rows_written",
                "dataset_duration_seconds",
            ],
            filtered,
        )
    except Exception as e:
        print_error(str(e))


@click.command()
@click.option("--process-id", required=True, type=int, help="Process ID.")
@click.option("--limit", default=10, type=int, help="Max executions to show.")
@click.option("--environment", default=None, help="Filter by environment.")
@click.option("--status-filter", "status_f", default=None, help="Filter by status.")
@click.pass_context
def history(ctx, process_id, limit, environment, status_f):
    """Show execution history for a process."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = ExecutionRepository(conn)
        rows = repo.get_execution_summary(
            process_id=process_id,
            environment=environment,
            status=status_f,
        )
        if not rows:
            click.echo("No execution history found.")
            return
        # Deduplicate to process-level summaries
        seen = set()
        summary = []
        for r in rows:
            pe_id = r.get("process_execution_id")
            if pe_id not in seen:
                seen.add(pe_id)
                summary.append(r)
                if len(summary) >= limit:
                    break
        print_table(
            f"Execution History (process_id={process_id})",
            [
                "process_execution_id", "process_status", "process_environment",
                "process_start_time", "process_end_time",
                "process_total_jobs", "process_completed_jobs", "process_failed_jobs",
            ],
            summary,
        )
    except Exception as e:
        print_error(str(e))


@click.command()
@click.argument("job_execution_id", type=int)
@click.pass_context
def retry(ctx, job_execution_id):
    """Retry failed datasets for a job execution."""
    try:
        conn = ctx.obj["get_connection"]()
        repo = ExecutionRepository(conn)
        rows = repo.retry_failed_datasets(job_execution_id)
        if not rows:
            click.echo("Nothing to retry.")
            return
        print_success(f"Retrying {len(rows)} dataset(s):")
        for r in rows:
            click.echo(
                f"  - {r.get('dataset_name')} (id={r.get('dataset_execution_id')}, "
                f"retry #{r.get('retry_count')})"
            )
    except Exception as e:
        print_error(str(e))
