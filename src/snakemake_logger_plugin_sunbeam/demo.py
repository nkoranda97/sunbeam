"""Standalone demo — simulates a workflow run so you can eyeball the UI.

Run:
    uv run python -m snakemake_logger_plugin_sunbeam.demo

Or, from the project root:
    uv run python src/snakemake_logger_plugin_sunbeam/demo.py
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

from snakemake_interface_logger_plugins.common import LogEvent

from snakemake_logger_plugin_sunbeam import LogHandler, LogHandlerSettings

AUTO_SECONDS = 60.0
TICK_SECONDS = 0.2
STAGE_HOLD_SECONDS = 12.0
FAILURE_TARGET = 19
WORKFLOW_ID = uuid.UUID("7a4f784f-b11d-46dd-8fd3-9db91a79c851")


def _settings(**overrides: Any) -> SimpleNamespace:
    s = SimpleNamespace(
        stdout=False,
        nocolor=False,
        verbose=False,
        printshellcmds=True,
        dryrun=False,
        quiet=None,
        show_failed_logs=True,
        debug_dag=False,
    )
    for k, v in overrides.items():
        setattr(s, k, v)
    return s


def _rec(event: LogEvent, **fields: Any) -> logging.LogRecord:
    r = logging.LogRecord("snakemake", logging.INFO, "wf.smk", 1, "", (), None)
    r.event = event
    for k, v in fields.items():
        setattr(r, k, v)
    return r


RULES = {
    "prep_reference": 1,
    "fastqc_raw": 4,
    "trim_adapters": 4,
    "fastqc_trimmed": 4,
    "salmon_quant": 4,
    "tximport": 1,
    "deseq2": 1,
    "multiqc": 1,
    "report": 1,
}

SAMPLES = ["HBR_Rep1", "HBR_Rep2", "UHR_Rep1", "UHR_Rep2"]

SHELLCMDS = {
    "prep_reference": "gffread annotation.gtf -g ref.fa -w transcripts.fa",
    "fastqc_raw": "fastqc -o results/qc/raw {input}",
    "trim_adapters": ("cutadapt -a AGATCGGAAGAGC -A AGATCGGAAGAGC -o {output.r1} -p {output.r2} {input.r1} {input.r2}"),
    "fastqc_trimmed": "fastqc -o results/qc/trimmed {input}",
    "salmon_quant": (
        "salmon quant -i resources/salmon_index --libType A "
        "-1 {input.r1} -2 {input.r2} -p 8 --validateMappings -o {output}"
    ),
    "tximport": "Rscript scripts/tximport.R",
    "deseq2": "Rscript scripts/deseq2.R",
    "multiqc": "multiqc -o results/multiqc results/",
    "report": "snakemake --report report.html",
}


@dataclass(frozen=True)
class DemoStage:
    name: str
    label: str
    done: int
    running: int
    failed: bool = False
    verbose: bool = False
    complete: bool = False


STAGES = [
    DemoStage("starting", "starting", done=0, running=0),
    DemoStage("running", "active workload", done=5, running=3),
    DemoStage("verbose", "verbose details", done=9, running=2, verbose=True),
    DemoStage("failure", "failure", done=16, running=1, failed=True),
    DemoStage("complete", "complete", done=21, running=0, complete=True),
]


def _jobs() -> list[dict[str, Any]]:
    jobs = []
    jid = 1
    for rule, count in RULES.items():
        for i in range(count):
            wc = SAMPLES[i % len(SAMPLES)] if count > 1 else ""
            jobs.append({"id": jid, "rule": rule, "wildcards": {"sample": wc} if wc else {}})
            jid += 1
    return jobs


def _job_fields(job: dict[str, Any]) -> dict[str, Any]:
    rule = job["rule"]
    sample = job["wildcards"].get("sample", "all")
    threads = 8 if rule == "salmon_quant" else 2
    return {
        "jobid": job["id"],
        "rule_name": rule,
        "threads": threads,
        "input": [f"data/{sample}.fastq.gz"] if sample != "all" else [],
        "output": [f"results/{rule}/{sample}"] if sample != "all" else [],
        "wildcards": job["wildcards"],
        "resources": {"mem_mb": 16000 if rule == "salmon_quant" else 4000},
        "reason": "demo snapshot",
    }


class DemoLogHandler(LogHandler):
    """Demo-only key shim; production LogHandler behavior stays unchanged."""

    def __init__(self, *args: Any, controller: DemoController, **kwargs: Any) -> None:
        self._demo_controller = controller
        self._demo_escape_state = 0
        super().__init__(*args, **kwargs)

    def _on_key(self, ch: str) -> None:
        if ch == "\x1b" and (self._filter_active or self._quit_pending):
            self._demo_escape_state = 0
            super()._on_key(ch)
            return

        if self._demo_escape_state == 1:
            self._demo_escape_state = 2 if ch == "[" else 0
            if self._demo_escape_state:
                return
        elif self._demo_escape_state == 2:
            self._demo_escape_state = 0
            if ch == "C":
                self._demo_controller.request_stage(1)
                return
            if ch == "D":
                self._demo_controller.request_stage(-1)
                return

        if ch == "\x1b":
            self._demo_escape_state = 1
            return
        if ch in ("n", "N"):
            self._demo_controller.request_stage(1)
            return
        if ch in ("p", "P"):
            self._demo_controller.request_stage(-1)
            return
        super()._on_key(ch)


class DemoController:
    def __init__(self) -> None:
        self._jobs = _jobs()
        self._lock = threading.Lock()
        self._pending_delta = 0
        self._manual = False
        self._stage_idx = 0
        self._handler: DemoLogHandler | None = None

    def request_stage(self, delta: int) -> None:
        with self._lock:
            self._pending_delta += delta
            self._manual = True

    def run(self) -> None:
        print("Demo controls: n/right next, p/left previous, q q quit")
        try:
            self._show_stage(0)
            started_at = time.time()
            next_auto_stage_at = started_at + STAGE_HOLD_SECONDS
            while self._handler is not None:
                time.sleep(TICK_SECONDS)
                delta = self._take_delta()
                if delta:
                    self._show_stage((self._stage_idx + delta) % len(STAGES))
                    next_auto_stage_at = time.time() + STAGE_HOLD_SECONDS
                    continue
                if self._is_manual:
                    continue
                now = time.time()
                if now - started_at >= AUTO_SECONDS:
                    self._show_stage(len(STAGES) - 1)
                    break
                if now >= next_auto_stage_at and self._stage_idx < len(STAGES) - 1:
                    self._show_stage(self._stage_idx + 1)
                    next_auto_stage_at = now + STAGE_HOLD_SECONDS
        except KeyboardInterrupt:
            pass
        finally:
            self._close_handler()

    @property
    def _is_manual(self) -> bool:
        with self._lock:
            return self._manual

    def _take_delta(self) -> int:
        with self._lock:
            delta = self._pending_delta
            self._pending_delta = 0
            return delta

    def _new_handler(self, *, verbose: bool = False) -> DemoLogHandler:
        return DemoLogHandler(
            common_settings=_settings(printshellcmds=True, verbose=verbose),
            settings=LogHandlerSettings(),
            controller=self,
        )

    def _close_handler(self) -> None:
        if self._handler is not None:
            handler = self._handler
            self._handler = None
            handler.close()

    def _show_stage(self, idx: int) -> None:
        self._close_handler()
        self._stage_idx = idx
        stage = STAGES[idx]
        handler = self._new_handler(verbose=stage.verbose)
        self._handler = handler
        self._seed_workflow(handler, stage)

    def _seed_workflow(self, handler: DemoLogHandler, stage: DemoStage) -> None:
        total = len(self._jobs)
        handler.emit(
            _rec(
                LogEvent.WORKFLOW_STARTED,
                snakefile="./rnaseq-quant/Snakefile",
                workflow_id=WORKFLOW_ID,
            )
        )
        handler.emit(_rec(LogEvent.RUN_INFO, per_rule_job_counts=RULES, total_job_count=total))
        handler.emit(_rec(LogEvent.RESOURCES_INFO, cores=8, nodes=["demo-node-01"]))
        handler.emit(_rec(LogEvent.PROGRESS, done=0, total=total))
        handler.emit(
            logging.LogRecord(
                "snakemake",
                logging.INFO,
                "demo.py",
                1,
                f"Demo stage: {stage.label} ({idx_label(self._stage_idx)})",
                (),
                None,
            )
        )

        failed_ids = {FAILURE_TARGET} if stage.failed else set()
        done_jobs = [j for j in self._jobs if j["id"] not in failed_ids][: stage.done]
        running_jobs = [j for j in self._jobs if j["id"] not in {d["id"] for d in done_jobs} | failed_ids][
            : stage.running
        ]

        for job in done_jobs:
            self._start_job(handler, job)
            handler.emit(_rec(LogEvent.JOB_FINISHED, job_id=job["id"]))

        for job in running_jobs:
            self._start_job(handler, job)

        if stage.failed:
            failed_job = self._jobs[FAILURE_TARGET - 1]
            self._start_job(handler, failed_job)
            handler.emit(_rec(LogEvent.JOB_ERROR, jobid=FAILURE_TARGET))
            handler.emit(
                _rec(
                    LogEvent.ERROR,
                    exception="MissingInputException: results/quant/HBR_Rep2/quant.sf",
                    rule=failed_job["rule"],
                    traceback="Traceback (most recent call last):\n  demo workflow failed intentionally",
                )
            )

        progress_done = len(done_jobs) + len(failed_ids)
        if stage.complete:
            handler._finished = True
            handler._refresh()
        else:
            handler.emit(_rec(LogEvent.PROGRESS, done=progress_done, total=total))

    def _start_job(self, handler: DemoLogHandler, job: dict[str, Any]) -> None:
        fields = _job_fields(job)
        handler.emit(_rec(LogEvent.JOB_INFO, **fields))
        handler.emit(
            _rec(
                LogEvent.SHELLCMD,
                shellcmd=SHELLCMDS.get(job["rule"], "echo hi"),
                rule_name=job["rule"],
                jobid=job["id"],
            )
        )
        handler.emit(_rec(LogEvent.JOB_STARTED, job_ids=[job["id"]]))


def idx_label(idx: int) -> str:
    return f"{idx + 1}/{len(STAGES)}"


def main() -> None:
    DemoController().run()


if __name__ == "__main__":
    main()
