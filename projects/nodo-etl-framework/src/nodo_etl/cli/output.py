"""Shared output formatting helpers for CLI commands."""

import click
from rich.console import Console
from rich.table import Table

console = Console()


def print_table(title: str, columns: list[str], rows: list[dict]) -> None:
    """Print a formatted table using Rich."""
    table = Table(title=title)
    for col in columns:
        table.add_column(col)
    for row in rows:
        table.add_row(*[str(row.get(col, "")) for col in columns])
    console.print(table)


def print_success(message: str) -> None:
    click.echo(click.style(message, fg="green"))


def print_error(message: str) -> None:
    click.echo(click.style(f"Error: {message}", fg="red"), err=True)


def print_warning(message: str) -> None:
    click.echo(click.style(f"Warning: {message}", fg="yellow"))


def mask_secret(value: str | None) -> str:
    """Mask a secret value for display."""
    if not value:
        return ""
    if len(value) <= 4:
        return "****"
    return value[:2] + "*" * (len(value) - 4) + value[-2:]
