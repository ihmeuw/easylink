import os
import subprocess
from ast import Str
from pathlib import Path
from typing import Any, Dict, List, Optional

from attr import s
from loguru import logger


def run_with_singularity(
    input_data: List[Path],
    results_dir: Path,
    diagnostics_dir: Path,
    container_path: Path,
    config: Optional[Dict[str, str]],
) -> None:
    logger.info("Running container with singularity")
    _run_container(input_data, results_dir, diagnostics_dir, container_path, config)
    _clean(results_dir, container_path)


def _run_container(
    input_data: List[Path],
    results_dir: Path,
    diagnostics_dir: Path,
    container_path: Path,
    config: Optional[Dict[str, str]],
) -> None:
    cmd = (
        "singularity run --containall --no-home --bind /tmp:/tmp "
        f"--bind {results_dir}:/results --bind {diagnostics_dir}:/diagnostics "
    )
    for filepath in input_data:
        cmd += f"--bind {str(filepath)}:/input_data/main_input_{str(filepath.name)} "
    cmd += f"{container_path}"
    _run_cmd(diagnostics_dir, cmd, config)


def _run_cmd(diagnostics_dir: Path, cmd: str, config: Optional[Dict[str, str]]) -> None:
    logger.debug(f"Command: {cmd}")
    # TODO: pipe this realtime to stdout (using subprocess.Popen I think)
    env_vars = os.environ.copy()
    if config:
        # NOTE: singularity < 3.6 does not support --env argument but supports variables
        #   prepended with 'SINGULARITYENV'
        env_config = {f"SINGULARITYENV_{key}": value for (key, value) in config.items()}
        env_vars.update(env_config)
    process = subprocess.run(
        cmd,
        shell=True,
        capture_output=True,
        text=True,
        env=env_vars,
    )
    if process.returncode != 0:
        raise RuntimeError(f"Error running command '{cmd}'\n" f"Error: {process.stderr}")

    with (diagnostics_dir / "singularity.o").open(mode="a") as output_file:
        output_file.write(f"{process.stdout}\n")
        output_file.write(process.stderr)


def _clean(results_dir: Path, container_path: Path) -> None:
    pass
    # TODO [MIC-4730]: do I need to clean up the cache?
