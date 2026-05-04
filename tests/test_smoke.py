"""End-to-end smoke test: plugin discovery and CLI round-trip via real Snakemake.

Skipped automatically when snakemake is not installed.
To run: `pip install snakemake` then `uv run pytest tests/test_smoke.py -v`
"""
from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

snakemake = pytest.importorskip("snakemake", reason="snakemake not installed")


@pytest.fixture()
def minimal_snakefile(tmp_path: Path) -> Path:
    snakefile = tmp_path / "Snakefile"
    snakefile.write_text(textwrap.dedent("""\
        rule all:
            input: "output.txt"

        rule make_output:
            output: "output.txt"
            shell: "echo hello > {output}"
    """))
    return snakefile


def test_plugin_discovery_and_basic_run(minimal_snakefile: Path, tmp_path: Path) -> None:
    """Snakemake can discover and use the sunbeam logger plugin end-to-end."""
    result = subprocess.run(
        [
            sys.executable, "-m", "snakemake",
            "--snakefile", str(minimal_snakefile),
            "--logger", "sunbeam",
            "--cores", "1",
            "--nocolor",
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"snakemake exited {result.returncode}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    assert (tmp_path / "output.txt").exists()


def test_plugin_with_theme_and_printshellcmds(minimal_snakefile: Path, tmp_path: Path) -> None:
    """Sunbeam accepts --logger-sunbeam-theme and --printshellcmds without crashing."""
    result = subprocess.run(
        [
            sys.executable, "-m", "snakemake",
            "--snakefile", str(minimal_snakefile),
            "--logger", "sunbeam",
            "--logger-sunbeam-theme", "vim",
            "--printshellcmds",
            "--cores", "1",
            "--nocolor",
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"snakemake exited {result.returncode}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )


def test_plugin_dryrun(minimal_snakefile: Path, tmp_path: Path) -> None:
    """Sunbeam handles --dryrun without executing anything."""
    result = subprocess.run(
        [
            sys.executable, "-m", "snakemake",
            "--snakefile", str(minimal_snakefile),
            "--logger", "sunbeam",
            "--dryrun",
            "--cores", "1",
            "--nocolor",
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f"snakemake exited {result.returncode}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    assert not (tmp_path / "output.txt").exists(), "dryrun should not create output files"
