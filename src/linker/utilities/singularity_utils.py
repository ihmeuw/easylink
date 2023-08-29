import subprocess
from pathlib import Path

from loguru import logger


def run_with_singularity(results_dir: Path, step_dir: Path) -> None:
    logger.info("Trying to run container with singularity")
    _build_container(results_dir, step_dir)
    _run_container(results_dir, step_dir)
    _clean(results_dir, step_dir)


def _build_container(results_dir: Path, step_dir: Path) -> None:
    if (results_dir / "image.sif").exists():
        raise RuntimeError(
            "Trying to build a Singularity image.sif but one already exists at "
            f"{results_dir}"
        )
    cmd = (
        f"singularity build {results_dir}/image.sif "
        f"docker-archive://{step_dir}/image.tar.gz"
    )
    logger.info("Building the singularity container from docker image")
    _run_cmd(results_dir, cmd)


def _run_container(results_dir: Path, step_dir: Path) -> None:
    cmd = (
        f"singularity run --pwd {step_dir} "
        f"--bind {results_dir}:/app/results "
        f"--bind {step_dir}/input_data:/app/input_data "
        f"{results_dir}/image.sif"
    )
    logger.info("Running the singularity container")
    _run_cmd(results_dir, cmd)


def _run_cmd(results_dir: Path, cmd: str) -> None:
    logger.debug(f"Command: {cmd}")
    # TODO: pipe this realtime to stdout (using subprocess.Popen I think)
    process = subprocess.run(
        cmd,
        shell=True,
        capture_output=True,
        text=True,
    )
    if process.returncode != 0:
        raise RuntimeError(f"Error running command {cmd}\n" f"Error: {process.stderr}")

    with (results_dir / "singularity.o").open(mode="a") as output_file:
        output_file.write(f"{process.stdout}\n")
        output_file.write(process.stderr)


def _clean(results_dir: Path, step_dir: Path) -> None:
    pass
    # TODO: do I need to clean up the cache?
