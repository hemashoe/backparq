"""Console output utilities."""

from __future__ import annotations

from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.table import Table

console = Console()
err_console = Console(stderr=True)


def create_progress() -> Progress:
    return Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(bar_width=40),
        TaskProgressColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
        transient=False,
    )


def print_header(title: str) -> None:
    console.print()
    console.rule(f"[bold blue]{title}", style="blue")
    console.print()


def print_success(msg: str) -> None:
    console.print(f"[green]OK[/green] {msg}")


def print_warning(msg: str) -> None:
    console.print(f"[yellow]WARN[/yellow] {msg}")


def print_error(msg: str) -> None:
    err_console.print(f"[red]ERROR[/red] {msg}")


def print_info(msg: str) -> None:
    console.print(f"[blue]INFO[/blue] {msg}")


def print_table(title: str, columns: list, rows: list) -> None:
    table = Table(title=title, show_header=True, header_style="bold")
    for col in columns:
        table.add_column(col)
    for row in rows:
        table.add_row(*[str(c) for c in row])
    console.print(table)


def print_stats(stats: dict) -> None:
    table = Table(title="Statistics", show_header=False, box=None)
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="green")
    for k, v in stats.items():
        table.add_row(k, str(v))
    console.print(table)


def format_size(size_bytes: int) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.1f} PB"


def format_count(count: int) -> str:
    return f"{count:,}"


def format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        return f"{seconds / 60:.1f}m"
    return f"{seconds / 3600:.1f}h"
