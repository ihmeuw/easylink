import ast
import os
import re
import tempfile
from pathlib import Path

import pytest

from linker.configuration import Config
from linker.utilities.slurm_utils import (
    _generate_job_template,
    _generate_spark_cluster_job_template,
    _get_cli_args,
    get_slurm_drmaa,
    is_on_slurm,
)

CLI_KWARGS = {
    "job_name": "some-job-name",
    "account": "some-account",
    "partition": "some-partition",
    "peak_memory": 42,
    "max_runtime": 24,
    "num_threads": 7,
}


IN_GITHUB_ACTIONS = os.getenv("GITHUB_ACTIONS") == "true"

@pytest.mark.skipif(
    IN_GITHUB_ACTIONS or not is_on_slurm(),
    reason="Must be on slurm and not in Github Actions to run this test.",
)
def test_get_slurm_drmaa():
    """Confirm that a drmaa object is indeed returned"""
    drmaa = get_slurm_drmaa()
    assert drmaa is not None
    assert drmaa.__name__ == "drmaa"


def test__get_cli_args():
    cli_args = _get_cli_args(**CLI_KWARGS)
    assert cli_args == (
        f"-J {CLI_KWARGS['job_name']} "
        f"-A {CLI_KWARGS['account']} "
        f"-p {CLI_KWARGS['partition']} "
        f"--mem={CLI_KWARGS['peak_memory']*1024} "
        f"-t {CLI_KWARGS['max_runtime']}:00:00 "
        f"-c {CLI_KWARGS['num_threads']}"
    )

    # Ensure that there are spaces between each item. When splitting on spaces,
    # we expect there to be 11 items (not 12 b/c "--mem=X" does not count as 2 items)
    assert len(cli_args.split(" ")) == 11


@pytest.mark.skipif(
    IN_GITHUB_ACTIONS or not is_on_slurm(),
    reason="Must be on slurm and not in Github Actions to run this test.",
)
def test__generate_job_template(default_config_params, mocker):
    slurm_kwargs = CLI_KWARGS.copy()
    # Change name from user requests to slurm requests
    slurm_kwargs["memory"] = slurm_kwargs.pop("peak_memory")
    slurm_kwargs["time_limit"] = slurm_kwargs.pop("max_runtime")
    slurm_kwargs["cpus"] = slurm_kwargs.pop("num_threads")
    mocker.patch(
        "linker.utilities.slurm_utils.Config.slurm_resources",
        return_value=slurm_kwargs,
        new_callable=mocker.PropertyMock,
    )

    job_template_kwargs = {
        "container_engine": "singularity",
        "input_data": ["input1", "input2"],
        "results_dir": Path("path/to/results"),
        "diagnostics_dir": Path("path/to/diagnostics"),
        "step_id": "some-step-id",
        "step_name": "some-step-name",
        "implementation_name": "some-implementation-name",
        "container_full_stem": "some-container-full-stem",
        "implementation_config": {"SOME_VAR_1": "env-var-1", "SOME_VAR_2": "env-var-2"},
    }
    config = Config(**default_config_params)
    drmaa = get_slurm_drmaa()
    session = drmaa.Session()
    session.initialize()

    jt = _generate_job_template(
        session,
        config,
        **job_template_kwargs,
    )

    # assert jt attributes are as expected
    assert re.match(rf"{job_template_kwargs['implementation_name']}_\d{{14}}$", jt.jobName)
    assert not jt.joinFiles
    assert jt.outputPath == f":{job_template_kwargs['diagnostics_dir']}/%A.o%a"
    assert jt.errorPath == f":{job_template_kwargs['diagnostics_dir']}/%A.e%a"
    assert jt.remoteCommand.split("/")[-1] == "linker"
    actual_args = jt.args
    # NOTE: this is pretty hacky and ordering is hard-coded
    expected_args = [
        "run-slurm-job",
        job_template_kwargs["container_engine"],
        str(job_template_kwargs["results_dir"]),
        str(job_template_kwargs["diagnostics_dir"]),
        job_template_kwargs["step_id"],
        job_template_kwargs["step_name"],
        job_template_kwargs["implementation_name"],
        job_template_kwargs["container_full_stem"],
        "-vvv",
    ]
    expected_args.extend(
        item
        for filename in job_template_kwargs["input_data"]
        for item in ("--input-data", filename)
    )
    expected_args.extend(
        ["--implementation-config", str(job_template_kwargs["implementation_config"])]
    )
    assert len(jt.args) == len(expected_args)
    # Special-case the implementation config since that's a stringified dict and hard to predict
    actual_config = actual_args.pop()
    expected_config = expected_args.pop()
    assert ast.literal_eval(actual_config) == ast.literal_eval(expected_config)
    assert actual_args == expected_args
    assert jt.jobEnvironment == {"LC_ALL": "en_US.UTF-8", "LANG": "en_US.UTF-8"}
    expected_native_specification = (
        f"-J {jt.jobName} "
        f"-A {CLI_KWARGS['account']} "
        f"-p {CLI_KWARGS['partition']} "
        f"--mem={CLI_KWARGS['peak_memory']*1024} "
        f"-t {CLI_KWARGS['max_runtime']}:00:00 "
        f"-c {CLI_KWARGS['num_threads']}"
    )
    assert jt.nativeSpecification == expected_native_specification
    session.exit()


@pytest.mark.skipif(
    IN_GITHUB_ACTIONS or not is_on_slurm(),
    reason="Must be on slurm and not in Github Actions to run this test.",
)
def test__generate_spark_cluster_jt(test_dir, mocker):
    launcher = tempfile.NamedTemporaryFile(
        mode="w",
        dir=Path(test_dir),
        prefix="spark_cluster_launcher_",
        suffix=".sh",
        delete=False,
    )
    job_template_kwargs = {
        "launcher": launcher,
        "diagnostics_dir": Path("path/to/diagnostics"),
        "step_id": "some-step-id",
    }
    mocker.patch(
        "linker.configuration.Config._determine_if_spark_is_required", return_value=True
    )
    config_params = {
        "pipeline_specification": Path(f"{test_dir}/pipeline.yaml"),
        "input_data": Path(f"{test_dir}/input_data.yaml"),
        "computing_environment": Path(f"{test_dir}/spark_environment.yaml"),
    }
    config = Config(**config_params)

    drmaa = get_slurm_drmaa()
    session = drmaa.Session()
    session.initialize()

    jt, _resources = _generate_spark_cluster_job_template(
        session,
        config,
        **job_template_kwargs,
    )

    # assert jt attributes are as expected
    assert re.match(rf"spark_cluster_{job_template_kwargs['step_id']}_\d{{14}}$", jt.jobName)
    assert jt.workingDirectory == str(job_template_kwargs["diagnostics_dir"])
    assert not jt.joinFiles
    assert (
        jt.outputPath
        == f":{job_template_kwargs['diagnostics_dir']}/spark_cluster_%A_%a.stdout"
    )
    assert (
        jt.errorPath
        == f":{job_template_kwargs['diagnostics_dir']}/spark_cluster_%A_%a.stderr"
    )
    assert jt.remoteCommand == "/bin/bash"
    assert jt.args == [launcher.name]
    assert jt.jobEnvironment == {"LC_ALL": "en_US.UTF-8", "LANG": "en_US.UTF-8"}
    expected_native_specification = (
        f"--account={config.slurm_resources['account']} "
        f"--partition={config.slurm_resources['partition']} "
        f"--mem={config.slurm_resources['memory']*1024} "
        f"--time={config.slurm_resources['time_limit']}:00:00 "
        f"--cpus-per-task={config.slurm_resources['cpus']}"
    )
    assert jt.nativeSpecification == expected_native_specification
    session.exit()


@pytest.mark.skip(reason="TODO: MIC-4915")
def test_slurm_resource_requests():
    pass
