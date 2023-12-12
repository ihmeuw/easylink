import subprocess
from pathlib import Path
from typing import List

from loguru import logger


def run_with_singularity(
    input_data: List[Path],
    results_dir: Path,
    log_dir: Path,
    container_path: Path,
) -> None:
    logger.info("Running container with singularity")
    _run_container(input_data, results_dir, log_dir, container_path)
    _clean(results_dir, container_path)


def _run_container(
    input_data: List[Path],
    results_dir: Path,
    log_dir: Path,
    container_path: Path,
) -> None:
    cmd = f"singularity run --containall --no-home --bind /tmp:/tmp --bind {results_dir}:/results --bind {log_dir}:/diagnostics"
    for filepath in input_data:
        cmd += f"--bind {str(filepath)}:/input_data/{str(filepath.name)} "
    cmd += f"{container_path}"
    _run_cmd(log_dir, cmd)


def _run_cmd(log_dir: Path, cmd: str) -> None:
    logger.debug(f"Command: {cmd}")
    # TODO: pipe this realtime to stdout (using subprocess.Popen I think)
    process = subprocess.run(
        cmd,
        shell=True,
        capture_output=True,
        text=True,
    )
    if process.returncode != 0:
        raise RuntimeError(f"Error running command '{cmd}'\n" f"Error: {process.stderr}")

    with (log_dir / "singularity.o").open(mode="a") as output_file:
        output_file.write(f"{process.stdout}\n")
        output_file.write(process.stderr)


def _clean(results_dir: Path, container_path: Path) -> None:
    pass
    # TODO [MIC-4730]: do I need to clean up the cache?
