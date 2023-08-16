import subprocess
from pathlib import Path

from loguru import logger


def build_singularity_container(results_dir: Path, step_dir: Path) -> None:
    cmd = (
        f"singularity build {step_dir}/image.sif "
        f"docker-archive://{step_dir}/image.tar.gz"
    )
    logger.info("Building the singularity container from docker image")
    _run_cmd(results_dir, cmd)


def run_singularity_container(results_dir: Path, step_dir: Path) -> None:
    cmd = (
        f"singularity run --pwd {step_dir} "
        f"--bind {results_dir}:/app/results "
        f"--bind {step_dir}/input_data:/app/input_data "
        f"{step_dir}/image.sif"
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


def clean_up_singularity(results_dir: Path, step_dir: Path) -> None:
    # Move singularity image file to results dir
    (step_dir / "image.sif").rename(results_dir / "image.sif")
    # TODO: do I need to clean up the cache?
