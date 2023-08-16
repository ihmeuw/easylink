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
    logger.info(f"Command: {cmd}")
    # TODO: pipe this realtime to stdout (using subprocess.Popen I think)
    output = subprocess.run(
        cmd,
        shell=True,
        capture_output=True,
        text=True,
    )
    with (results_dir / "singularity.o").open(mode="a") as output_file:
        output_file.write(f"{output.stdout}\n")
        output_file.write(output.stderr)


def clean_up_singularity(results_dir: Path, step_dir: Path) -> None:
    # Move singularity image file to results dir
    (step_dir / "image.sif").rename(results_dir / "image.sif")
    # TODO: do I need to clean up the cache?
