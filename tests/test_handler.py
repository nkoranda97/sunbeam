import logging
import time
import uuid
from typing import Type

from rich.console import Console
from snakemake_interface_logger_plugins.base import LogHandlerBase
from snakemake_interface_logger_plugins.common import LogEvent
from snakemake_interface_logger_plugins.settings import LogHandlerSettingsBase
from snakemake_interface_logger_plugins.tests import MockOutputSettings, TestLogHandlerBase

from snakemake_logger_plugin_sunbeam import LogHandler, LogHandlerSettings


def make_record(event: LogEvent, msg: str = "", **kwargs) -> logging.LogRecord:
    record = logging.LogRecord(
        name="snakemake", level=logging.INFO,
        pathname="workflow.smk", lineno=1,
        msg=msg, args=(), exc_info=None,
    )
    record.event = event
    for k, v in kwargs.items():
        setattr(record, k, v)
    return record


def make_handler(
    *,
    verbose: bool = False,
    printshellcmds: bool = True,
    dryrun: bool = False,
    quiet=None,
    show_failed_logs: bool = True,
    debug_dag: bool = False,
) -> tuple[LogHandler, Console]:
    settings = MockOutputSettings()
    settings.verbose = verbose
    settings.printshellcmds = printshellcmds
    settings.dryrun = dryrun
    settings.quiet = quiet
    settings.show_failed_logs = show_failed_logs
    settings.debug_dag = debug_dag
    handler = LogHandler(common_settings=settings, settings=LogHandlerSettings())
    recording_console = Console(record=True, width=120, no_color=True)
    handler.console = recording_console
    return handler, recording_console


class TestSunbeamLogHandler(TestLogHandlerBase):
    __test__ = True

    def get_log_handler_cls(self) -> Type[LogHandlerBase]:
        return LogHandler

    def get_log_handler_settings(self) -> LogHandlerSettingsBase:
        return LogHandlerSettings()

    def test_workflow_started(self):
        handler, console = make_handler()
        handler.emit(make_record(
            LogEvent.WORKFLOW_STARTED,
            workflow_id=uuid.uuid4(),
            snakefile="Snakefile",
        ))
        output = console.export_text(clear=False)
        assert "Workflow Started" in output
        # Snakefile path is stored in handler state and shown in TUI banner.
        assert handler._snakefile == "Snakefile"

    def test_workflow_started_dryrun_label(self):
        handler, console = make_handler(dryrun=True)
        handler.emit(make_record(LogEvent.WORKFLOW_STARTED))
        output = console.export_text(clear=False)
        assert "DRY RUN" in output

    def test_run_info(self):
        handler, console = make_handler()
        handler.emit(make_record(
            LogEvent.RUN_INFO,
            per_rule_job_counts={"rule_a": 3, "rule_b": 1},
            total_job_count=4,
        ))
        output = console.export_text(clear=False)
        assert "rule_a" in output
        assert "3" in output
        assert "4" in output

    def test_shellcmd_hidden_when_disabled(self):
        handler, console = make_handler(printshellcmds=False)
        handler.emit(make_record(LogEvent.SHELLCMD, shellcmd="echo hello", rule_name="r"))
        assert "echo hello" not in console.export_text(clear=False)

    def test_shellcmd_shown_when_enabled(self):
        handler, console = make_handler(printshellcmds=True)
        handler.emit(make_record(LogEvent.SHELLCMD, shellcmd="echo hello", rule_name="r"))
        # Shell command is stored in _last_shell for rendering in the TUI frame.
        assert handler._last_shell is not None
        assert "echo hello" in handler._last_shell[1]

    def test_job_info_non_verbose(self):
        handler, console = make_handler(verbose=False)
        handler.emit(make_record(
            LogEvent.JOB_INFO, jobid=42, rule_name="align",
            threads=4, input=["a.fastq"], output=["a.bam"],
        ))
        # Non-verbose: job stored in _job_specs for TUI rendering, not printed.
        assert 42 in handler._job_specs
        assert handler._job_specs[42].rule == "align"
        # Verbose detail must not appear anywhere.
        assert "Input" not in console.export_text(clear=False)

    def test_job_info_verbose(self):
        handler, console = make_handler(verbose=True)
        handler.emit(make_record(
            LogEvent.JOB_INFO, jobid=42, rule_name="align",
            threads=4, input=["a.fastq"], output=["a.bam"],
        ))
        output = console.export_text(clear=False)
        assert "Input" in output
        assert "a.fastq" in output

    def test_job_info_quiet_suppression(self):
        handler, console = make_handler(quiet=["align"])
        handler.emit(make_record(
            LogEvent.JOB_INFO, jobid=42, rule_name="align", threads=1,
        ))
        assert "align" not in console.export_text(clear=False)

    def test_error_renders_panel(self):
        handler, console = make_handler()
        handler.emit(make_record(
            LogEvent.ERROR,
            exception="ValueError: bad input",
            rule="my_rule",
            traceback="Traceback...",
        ))
        output = console.export_text(clear=False)
        assert "Error" in output
        assert "ValueError" in output

    def test_job_error_removes_active_job(self):
        from snakemake_logger_plugin_sunbeam import _JobEntry
        handler, console = make_handler()
        handler._ensure_live()
        handler._active_jobs[5] = _JobEntry(job_id=5, rule="align")
        handler.emit(make_record(LogEvent.JOB_ERROR, jobid=5))
        assert 5 not in handler._active_jobs
        handler._stop_live()

    def test_progress_lifecycle(self):
        handler, _ = make_handler()
        handler.emit(make_record(LogEvent.PROGRESS, done=3, total=10))
        assert handler._live is not None
        handler.emit(make_record(LogEvent.PROGRESS, done=10, total=10))
        assert handler._live is None

    def test_emit_never_raises_on_bad_record(self):
        handler, _ = make_handler()
        record = logging.LogRecord("x", logging.INFO, "f.py", 1, "msg", (), None)
        handler.emit(record)

    def test_group_error_renders_panel(self):
        handler, console = make_handler(show_failed_logs=True)
        handler.emit(make_record(
            LogEvent.GROUP_ERROR,
            groupid=1,
            aux_logs=["log line 1"],
            job_error_info={"err": "something"},
        ))
        output = console.export_text(clear=False)
        assert "Group Error" in output

    def test_resources_info(self):
        handler, console = make_handler()
        handler.emit(make_record(LogEvent.RESOURCES_INFO, cores=8, nodes=["node1", "node2"]))
        # Resources are stored in handler state for TUI rendering, not printed directly.
        assert handler._cores == 8
        assert "node1" in handler._nodes

    def test_rulegraph_is_noop(self):
        handler, console = make_handler()
        handler.emit(make_record(LogEvent.RULEGRAPH, rulegraph={}))

    def test_workflow_summary_success(self):
        handler, console = make_handler()
        # Simulate 2 finished jobs then workflow complete
        handler.emit(make_record(LogEvent.JOB_FINISHED, job_id=1))
        handler.emit(make_record(LogEvent.JOB_FINISHED, job_id=2))
        handler.emit(make_record(LogEvent.PROGRESS, done=2, total=2))
        output = console.export_text(clear=False)
        assert "Workflow Complete" in output
        assert "2" in output

    def test_workflow_summary_failure(self):
        handler, console = make_handler()
        handler.emit(make_record(LogEvent.JOB_FINISHED, job_id=1))
        handler.emit(make_record(LogEvent.JOB_ERROR, jobid=2))
        handler.emit(make_record(LogEvent.PROGRESS, done=2, total=2))
        output = console.export_text(clear=False)
        assert "Workflow Failed" in output

    def test_workflow_summary_includes_elapsed(self):
        handler, console = make_handler()
        handler._workflow_start_time = time.time() - 5.0  # fake 5s elapsed
        handler._jobs_done = 3
        handler._print_workflow_summary()
        output = console.export_text(clear=False)
        assert "s" in output  # e.g. "5.0s"
        assert "3" in output

    def test_theme_applied_to_shellcmd_syntax(self):
        from snakemake_interface_logger_plugins.tests import MockOutputSettings
        settings = MockOutputSettings()
        settings.printshellcmds = True
        handler = LogHandler(
            common_settings=settings,
            settings=LogHandlerSettings(theme="vim"),
        )
        assert handler._syntax_theme == "vim"

    def test_default_syntax_theme(self):
        handler, _ = make_handler()
        assert handler._syntax_theme == "monokai"

    def test_close_stops_live(self):
        handler, _ = make_handler()
        handler._ensure_live()
        assert handler._live is not None
        handler.close()
        assert handler._live is None

    def test_debug_dag_hidden_by_default(self):
        handler, console = make_handler(debug_dag=False)
        handler.emit(make_record(LogEvent.DEBUG_DAG, status="ready", file="some.txt"))
        assert "DAG" not in console.export_text(clear=False)

    def test_debug_dag_shown_when_enabled(self):
        handler, console = make_handler(debug_dag=True)
        handler.emit(make_record(LogEvent.DEBUG_DAG, status="ready", file="some.txt"))
        assert "DAG" in console.export_text(clear=False)
