"""Standalone demo — simulates a workflow run so you can eyeball the UI.

Run:
    uv run python -m snakemake_logger_plugin_sunbeam.demo

Or, from the project root:
    uv run python src/snakemake_logger_plugin_sunbeam/demo.py
"""

from __future__ import annotations

import logging
import random
import time
import uuid
from types import SimpleNamespace

from snakemake_interface_logger_plugins.common import LogEvent

from snakemake_logger_plugin_sunbeam import LogHandler, LogHandlerSettings


def _settings(**overrides):
    s = SimpleNamespace(
        stdout=False, nocolor=False, verbose=False,
        printshellcmds=True, dryrun=False, quiet=None,
        show_failed_logs=True, debug_dag=False,
    )
    for k, v in overrides.items():
        setattr(s, k, v)
    return s


def _rec(event, **fields):
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
    "fastqc_raw":     "fastqc -o results/qc/raw {input}",
    "trim_adapters":  "cutadapt -a AGATCGGAAGAGC -A AGATCGGAAGAGC -o {output.r1} -p {output.r2} {input.r1} {input.r2}",
    "fastqc_trimmed": "fastqc -o results/qc/trimmed {input}",
    "salmon_quant":   ("salmon quant -i resources/salmon_index --libType A "
                       "-1 {input.r1} -2 {input.r2} -p 8 --validateMappings -o {output}"),
    "tximport":       "Rscript scripts/tximport.R",
    "deseq2":         "Rscript scripts/deseq2.R",
    "multiqc":        "multiqc -o results/multiqc results/",
    "report":         "snakemake --report report.html",
}


def main() -> None:
    random.seed(7)
    handler = LogHandler(common_settings=_settings(printshellcmds=True), settings=LogHandlerSettings())

    # Build the job list in dependency order.
    jobs = []
    jid = 1
    for rule, count in RULES.items():
        for i in range(count):
            wc = SAMPLES[i % len(SAMPLES)] if count > 1 else ""
            jobs.append({"id": jid, "rule": rule, "wildcards": {"sample": wc} if wc else {}})
            jid += 1
    total = len(jobs)

    handler.emit(_rec(LogEvent.WORKFLOW_STARTED, snakefile="./rnaseq-quant/Snakefile", workflow_id=uuid.uuid4()))
    handler.emit(_rec(LogEvent.RUN_INFO, per_rule_job_counts=RULES, total_job_count=total))
    handler.emit(_rec(LogEvent.RESOURCES_INFO, cores=8, nodes=[]))
    handler.emit(_rec(LogEvent.PROGRESS, done=0, total=total))

    pending = list(jobs)
    running = []
    done_count = 0
    failed_count = 0
    failure_target = 19  # simulate a failure on this job id for demo color
    MAX_PARALLEL = 4

    while pending or running:
        # Launch up to MAX_PARALLEL
        while pending and len(running) < MAX_PARALLEL:
            j = pending.pop(0)
            handler.emit(_rec(LogEvent.JOB_INFO, jobid=j["id"], rule_name=j["rule"],
                              threads=8 if j["rule"] == "salmon_quant" else 2,
                              input=[], output=[],
                              wildcards=j["wildcards"],
                              resources={"mem_mb": 4000}))
            handler.emit(_rec(LogEvent.SHELLCMD, shellcmd=SHELLCMDS.get(j["rule"], "echo hi"),
                              rule_name=j["rule"], jobid=j["id"]))
            handler.emit(_rec(LogEvent.JOB_STARTED, job_ids=[j["id"]]))
            j["_finish_at"] = time.time() + random.uniform(0.4, 1.4)
            running.append(j)

        # Tick
        time.sleep(0.12)
        now = time.time()
        still_running = []
        for j in running:
            if now >= j["_finish_at"]:
                if j["id"] == failure_target:
                    handler.emit(_rec(LogEvent.JOB_ERROR, jobid=j["id"]))
                    failed_count += 1
                else:
                    handler.emit(_rec(LogEvent.JOB_FINISHED, job_id=j["id"]))
                    done_count += 1
            else:
                still_running.append(j)
        running = still_running
        # Progress tick so the frame refreshes throughput
        handler.emit(_rec(LogEvent.PROGRESS, done=done_count + failed_count, total=total))

    handler.emit(_rec(LogEvent.PROGRESS, done=total, total=total))
    handler.close()


if __name__ == "__main__":
    main()
