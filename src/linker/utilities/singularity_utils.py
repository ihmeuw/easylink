import json
import os
import subprocess
from pathlib import Path
from typing import Dict, List, Optional

from loguru import logger

from linker.step import StepInput


def run_with_singularity(
    step_inputs: List[StepInput],
    results_dir: Path,
    diagnostics_dir: Path,
    step_id: str,
    container_path: Path,
    config: Optional[Dict[str, str]],
) -> None:
    logger.info(f"Running step {step_id} container with singularity")
    _run_container(step_inputs, results_dir, diagnostics_dir, container_path, config)
    _clean(results_dir, container_path)


def _run_container(
    step_inputs: List[StepInput],
    results_dir: Path,
    diagnostics_dir: Path,
    container_path: Path,
    config: Optional[Dict[str, str]],
) -> None:
    cmd = (
        "singularity run --containall --no-home --bind /tmp:/tmp "
        f"--bind {results_dir}:/results --bind {diagnostics_dir}:/diagnostics "
    )
    for step_input in step_inputs:
        for outside_path, inside_path in step_input.bindings.items():
            cmd += f"--bind {outside_path}:{inside_path} "
    cmd += f"{container_path}"
    _run_cmd(diagnostics_dir, cmd, config, step_inputs)


def _run_cmd(
    diagnostics_dir: Path,
    cmd: str,
    config: Optional[Dict[str, str]],
    step_inputs: List[StepInput],
) -> None:
    logger.debug(f"Command: {cmd}")
    # TODO: pipe this realtime to stdout (using subprocess.Popen I think)
    env_vars = os.environ.copy()
    if config:
        # NOTE: singularity < 3.6 does not support --env argument but supports variables
        #   prepended with 'SINGULARITYENV_'
        env_config = {f"SINGULARITYENV_{key}": value for (key, value) in config.items()}
        env_vars.update(env_config)
    step_vars = {
        f"SINGULARITYENV_{input.env_var}": json.dumps(input.container_paths)
        for input in step_inputs
    }
    env_vars.update(step_vars)
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
