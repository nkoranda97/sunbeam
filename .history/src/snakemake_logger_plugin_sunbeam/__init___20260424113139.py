from __future__ import annotations

import sys
import time

try:
    import termios
    _HAS_TERMIOS = True
except ImportError:
    _HAS_TERMIOS = False
from dataclasses import dataclass, field
from logging import LogRecord
from typing import Callable, Optional

from rich import box
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.progress import ProgressBar
from rich.rule import Rule
from rich.spinner import Spinner
from rich.syntax import Syntax
from rich.table import Table
from rich.text import Text
from snakemake_interface_logger_plugins.base import LogHandlerBase
from snakemake_interface_logger_plugins.common import LogEvent
from snakemake_interface_logger_plugins.settings import (
    LogHandlerSettingsBase,
    OutputSettingsLoggerInterface,
)


@dataclass
class LogHandlerSettings(LogHandlerSettingsBase):
    theme: Optional[str] = field(
        default=None,
        metadata={"help": "Name of the rich theme to use for output.", "type": str},
    )


class LogHandler(LogHandlerBase):
    def __init__(
        self,
        common_settings: OutputSettingsLoggerInterface,
        settings: Optional[LogHandlerSettings] = None,
    ) -> None:
        super().__init__(common_settings=common_settings, settings=settings)

    def __post_init__(self) -> None:
        self.console = Console(
            file=sys.stdout if self.common_settings.stdout else sys.stderr,
            no_color=self.common_settings.nocolor,
        )
        self._live: Optional[Live] = None
        self._old_tty_settings: Optional[list] = None
        self._active_jobs: dict[int, tuple[str, Spinner]] = {}
        self._job_rule_names: dict[int, str] = {}
        self._total_jobs: int = 0
        self._workflow_start_time: Optional[float] = None
        self._jobs_done: int = 0
        self._jobs_failed: int = 0
        self._syntax_theme: str = (
            self.settings.theme
            if self.settings and self.settings.theme
            else "monokai"
        )
        self._dispatch: dict[str, Callable[[LogRecord], None]] = {
            LogEvent.RUN_INFO: self._handle_run_info,
            LogEvent.WORKFLOW_STARTED: self._handle_workflow_started,
            LogEvent.SHELLCMD: self._handle_shellcmd,
            LogEvent.JOB_INFO: self._handle_job_info,
            LogEvent.JOB_STARTED: self._handle_job_started,
            LogEvent.JOB_FINISHED: self._handle_job_finished,
            LogEvent.JOB_ERROR: self._handle_job_error,
            LogEvent.GROUP_INFO: self._handle_group_info,
            LogEvent.GROUP_ERROR: self._handle_group_error,
            LogEvent.RESOURCES_INFO: self._handle_resources_info,
            LogEvent.DEBUG_DAG: self._handle_debug_dag,
            LogEvent.PROGRESS: self._handle_progress,
            LogEvent.RULEGRAPH: self._handle_rulegraph,
            LogEvent.ERROR: self._handle_error,
        }

    def emit(self, record: LogRecord) -> None:
        try:
            event = getattr(record, "event", None)
            handler = self._dispatch.get(event)
            if handler is not None:
                handler(record)
            else:
                self._print(self.format(record))
        except Exception:
            self.handleError(record)

    # --- Output routing ---

    def _print(self, *args, **kwargs) -> None:
        if self._live is not None and self._live.is_started:
            self._live.console.print(*args, **kwargs)
        else:
            self.console.print(*args, **kwargs)

    # --- Live display ---

    def _make_display(self) -> Panel:
        running = len(self._active_jobs)
        total = self._total_jobs
        done = self._jobs_done
        failed = self._jobs_failed
        elapsed = ""
        if self._workflow_start_time is not None:
            secs = int(time.time() - self._workflow_start_time)
            elapsed = f" · {secs // 60}:{secs % 60:02d}"

        stats = Table(box=box.SIMPLE_HEAD, show_edge=False, padding=(0, 2))
        stats.add_column("Total", justify="center")
        stats.add_column("Completed", justify="center", style="green")
        stats.add_column("Running", justify="center", style="cyan")
        stats.add_column("Failed", justify="center", style="red" if failed else "dim")
        stats.add_column("Progress")
        stats.add_column("", style="dim")
        bar = ProgressBar(total=max(total, 1), completed=done, width=24)
        stats.add_row(str(total), str(done), str(running), str(failed), bar, f"{done}/{total}")

        parts: list = [stats]

        if self._active_jobs:
            jobs = Table(box=None, show_header=False, padding=(0, 1), show_edge=False)
            jobs.add_column(width=2)
            jobs.add_column(style="cyan", no_wrap=True)
            jobs.add_column(style="dim")
            for jid, (rule, spinner) in sorted(self._active_jobs.items()):
                jobs.add_row(spinner, rule, f"#{jid}")
            parts.append(jobs)

        return Panel(
            Group(*parts),
            title=f"[bold cyan]Workflow Progress[/]{elapsed}",
            border_style="cyan",
        )

    def _ensure_live_started(self, total: int) -> None:
        if self._live is None:
            self._total_jobs = total
            self._live = Live(
                self._make_display(),
                console=self.console,
                refresh_per_second=4,
                transient=False,
            )
            self._live.start()
            if _HAS_TERMIOS and sys.stdin.isatty():
                try:
                    self._old_tty_settings = termios.tcgetattr(sys.stdin)
                    new = termios.tcgetattr(sys.stdin)
                    new[3] &= ~termios.ECHO
                    termios.tcsetattr(sys.stdin, termios.TCSADRAIN, new)
                except termios.error:
                    self._old_tty_settings = None

    def _stop_live(self) -> None:
        if self._live is not None:
            try:
                self._live.stop()
            finally:
                self._live = None
                self._active_jobs.clear()
                self._job_rule_names.clear()
                self._total_jobs = 0
                if _HAS_TERMIOS and self._old_tty_settings is not None:
                    try:
                        termios.tcsetattr(sys.stdin, termios.TCSADRAIN, self._old_tty_settings)
                    except termios.error:
                        pass
                    finally:
                        self._old_tty_settings = None

    def _print_workflow_summary(self) -> None:
        elapsed_str = ""
        if self._workflow_start_time is not None:
            secs = time.time() - self._workflow_start_time
            elapsed_str = f" in {secs:.1f}s"
        if self._jobs_failed > 0:
            msg = (
                f"[bold red]{self._jobs_failed} job(s) failed[/], "
                f"{self._jobs_done} completed{elapsed_str}"
            )
            self._print(Panel(msg, title="Workflow Failed", border_style="red"))
        else:
            msg = f"[bold green]{self._jobs_done} job(s) completed[/]{elapsed_str}"
            self._print(Panel(msg, title="Workflow Complete", border_style="green"))
        self._jobs_done = 0
        self._jobs_failed = 0
        self._workflow_start_time = None

    def close(self) -> None:
        self._stop_live()
        super().close()

    # --- Event handlers ---

    def _handle_workflow_started(self, record: LogRecord) -> None:
        snakefile = getattr(record, "snakefile", None)
        workflow_id = getattr(record, "workflow_id", None)
        dryrun_tag = " [dim][DRY RUN][/]" if self.common_settings.dryrun else ""
        title = f"[bold green]Workflow Started[/]{dryrun_tag}"
        self._workflow_start_time = time.time()
        self._print(Rule(title, style="green"))
        parts: list[str] = []
        if snakefile:
            parts.append(f"[bold]Snakefile:[/] {snakefile}")
        if workflow_id:
            parts.append(f"[bold]Workflow ID:[/] {workflow_id}")
        if parts:
            self._print("\n".join(parts))

    def _handle_run_info(self, record: LogRecord) -> None:
        per_rule: dict[str, int] = getattr(record, "per_rule_job_counts", {})
        total: int = getattr(record, "total_job_count", 0)
        table = Table(title="Job Counts", box=box.SIMPLE)
        table.add_column("Rule", style="cyan")
        table.add_column("Jobs", justify="right", style="bold")
        for rule_name, count in sorted(per_rule.items()):
            table.add_row(rule_name, str(count))
        table.add_row("[bold]Total[/]", f"[bold]{total}[/]")
        self._print(table)

    def _handle_resources_info(self, record: LogRecord) -> None:
        cores = getattr(record, "cores", None)
        nodes = getattr(record, "nodes", None)
        provided = getattr(record, "provided_resources", None)
        parts: list[str] = []
        if cores is not None:
            parts.append(f"[bold]Cores:[/] {cores}")
        if nodes:
            parts.append(f"[bold]Nodes:[/] {', '.join(nodes)}")
        if provided:
            parts.append(f"[bold]Resources:[/] {provided}")
        if parts:
            self._print(Text.from_markup(" | ".join(parts), style="dim"))

    def _handle_shellcmd(self, record: LogRecord) -> None:
        if not self.common_settings.printshellcmds:
            return
        rule_name = getattr(record, "rule_name", None)
        shellcmd = getattr(record, "shellcmd", None)
        if shellcmd:
            title = f"Shell: {rule_name}" if rule_name else "Shell Command"
            self._print(Panel(Syntax(shellcmd, "bash", theme=self._syntax_theme), title=title, border_style="dim blue"))

    def _handle_job_info(self, record: LogRecord) -> None:
        rule_name: str = getattr(record, "rule_name", "?")
        quiet = self.common_settings.quiet
        if quiet and rule_name in quiet:
            return
        jobid: int = getattr(record, "jobid", -1)
        self._job_rule_names[jobid] = rule_name
        threads: int = getattr(record, "threads", 1)
        header = f"[bold cyan]Job {jobid}[/] [dim]·[/] [bold]{rule_name}[/]"
        if not self.common_settings.verbose:
            self._print(header)
            return
        table = Table(show_header=False, box=box.SIMPLE, padding=(0, 1))
        table.add_column("Field", style="dim")
        table.add_column("Value")
        table.add_row("Rule", rule_name)
        table.add_row("Threads", str(threads))
        inp = getattr(record, "input", None)
        out = getattr(record, "output", None)
        wildcards = getattr(record, "wildcards", None)
        reason = getattr(record, "reason", None)
        resources = getattr(record, "resources", None)
        if inp:
            table.add_row("Input", ", ".join(inp))
        if out:
            table.add_row("Output", ", ".join(out))
        if wildcards:
            table.add_row("Wildcards", str(wildcards))
        if reason:
            table.add_row("Reason", reason)
        if resources:
            table.add_row("Resources", str(resources))
        shellcmd = getattr(record, "shellcmd", None)
        if shellcmd and self.common_settings.printshellcmds:
            self._print(Panel(
                Group(table, Syntax(shellcmd, "bash", theme=self._syntax_theme)),
                title=header,
                border_style="blue",
            ))
        else:
            self._print(Panel(table, title=header, border_style="blue"))

    def _handle_job_started(self, record: LogRecord) -> None:
        job_ids: list[int] = getattr(record, "job_ids", [])
        if self._live is None:
            return
        for jid in job_ids:
            rule = self._job_rule_names.get(jid, f"job_{jid}")
            self._active_jobs[jid] = (rule, Spinner("dots"))
        self._live.update(self._make_display())

    def _handle_job_finished(self, record: LogRecord) -> None:
        job_id: int = getattr(record, "job_id", -1)
        self._active_jobs.pop(job_id, None)
        self._jobs_done += 1
        if self._live is not None:
            self._live.update(self._make_display())

    def _handle_job_error(self, record: LogRecord) -> None:
        jobid: int = getattr(record, "jobid", -1)
        self._active_jobs.pop(jobid, None)
        self._jobs_failed += 1
        self._print(Panel(
            f"[bold red]Job {jobid} failed[/]",
            border_style="red",
            title="Job Error",
        ))
        if self._live is not None:
            self._live.update(self._make_display())

    def _handle_group_info(self, record: LogRecord) -> None:
        group_id = getattr(record, "group_id", "?")
        self._print(f"[dim]Group {group_id} started[/]")

    def _handle_group_error(self, record: LogRecord) -> None:
        groupid = getattr(record, "groupid", "?")
        job_error_info = getattr(record, "job_error_info", {})
        aux_logs = getattr(record, "aux_logs", [])
        lines: list[str] = [f"Group {groupid} failed"]
        if job_error_info:
            lines.append(str(job_error_info))
        if aux_logs and self.common_settings.show_failed_logs:
            for log in aux_logs:
                lines.append(str(log))
        self._print(Panel("\n".join(lines), title="Group Error", border_style="red"))

    def _handle_progress(self, record: LogRecord) -> None:
        done: int = getattr(record, "done", 0)
        total: int = getattr(record, "total", 0)
        if total > 0:
            self._ensure_live_started(total)
        if self._live is not None:
            self._total_jobs = total
            self._live.update(self._make_display())
        if total > 0 and done >= total:
            self._stop_live()
            self._print_workflow_summary()

    def _handle_error(self, record: LogRecord) -> None:
        exception = getattr(record, "exception", None)
        traceback_str = getattr(record, "traceback", None)
        location = getattr(record, "location", None)
        rule = getattr(record, "rule", None)
        file_ = getattr(record, "file", None)
        line = getattr(record, "line", None)
        msg = record.getMessage()
        parts: list = []
        if rule:
            parts.append(Text(f"Rule: {rule}", style="bold red"))
        if location:
            parts.append(Text(f"Location: {location}", style="dim"))
        elif file_:
            loc = f"{file_}:{line}" if line else file_
            parts.append(Text(f"File: {loc}", style="dim"))
        if exception:
            parts.append(Text(str(exception), style="bold red"))
        if msg:
            parts.append(Text(msg, style="red"))
        if traceback_str:
            parts.append(Syntax(str(traceback_str), "python", theme=self._syntax_theme))
        self._print(Panel(
            Group(*parts) if parts else Text("An error occurred", style="red"),
            title="[bold red]Error[/]",
            border_style="red",
        ))

    def _handle_debug_dag(self, record: LogRecord) -> None:
        if not getattr(self.common_settings, "debug_dag", False):
            return
        status = getattr(record, "status", "")
        file_ = getattr(record, "file", "")
        self._print(f"[dim][DAG] {status} {file_}[/]")

    def _handle_rulegraph(self, record: LogRecord) -> None:
        pass  # v1: not implemented

    # --- Abstract properties ---

    @property
    def writes_to_stream(self) -> bool:
        return True

    @property
    def writes_to_file(self) -> bool:
        return False

    @property
    def has_filter(self) -> bool:
        return False

    @property
    def has_formatter(self) -> bool:
        return False

    @property
    def needs_rulegraph(self) -> bool:
        return False
