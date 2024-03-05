import ast
import re
from pathlib import Path

from linker.configuration import Config
from linker.utilities.slurm_utils import (
    _generate_job_template,
    _get_cli_args,
    get_slurm_drmaa,
)

CLI_KWARGS = {
    "job_name": "some-job-name",
    "account": "some-account",
    "partition": "some-partition",
    "peak_memory": 42,
    "max_runtime": 24,
    "num_threads": 7,
}


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


def test__generate_job_template(default_config_params, mocker):
    mocked_return_value = CLI_KWARGS.copy()
    mocked_return_value["memory"] = mocked_return_value.pop("peak_memory")
    mocked_return_value["time_limit"] = mocked_return_value.pop("max_runtime")
    mocked_return_value["cpus"] = mocked_return_value.pop("num_threads")
    mocker.patch(
        "linker.utilities.slurm_utils.Config.slurm_resources",
        return_value=mocked_return_value,
        new_callable=mocker.PropertyMock,
    )

    kwargs = {
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
        **kwargs,
    )

    # assert jt attributes are as expected
    assert re.match(rf"{kwargs['implementation_name']}_\d{{14}}$", jt.jobName)
    assert not jt.joinFiles
    assert jt.outputPath == f":{kwargs['diagnostics_dir']}/%A.o%a"
    assert jt.errorPath == f":{kwargs['diagnostics_dir']}/%A.e%a"
    assert jt.remoteCommand.split("/")[-1] == "linker"
    actual_args = jt.args
    # NOTE: this is pretty hacky and ordering is hard-coded
    expected_args = [
        "run-slurm-job",
        kwargs["container_engine"],
        str(kwargs["results_dir"]),
        str(kwargs["diagnostics_dir"]),
        kwargs["step_id"],
        kwargs["step_name"],
        kwargs["implementation_name"],
        kwargs["container_full_stem"],
        "-vvv",
    ]
    expected_args.extend(
        item for filename in kwargs["input_data"] for item in ("--input-data", filename)
    )
    expected_args.extend(["--implementation-config", str(kwargs["implementation_config"])])
    assert len(jt.args) == len(expected_args)
    # Special-case the implementation config since that's a stringified dict and hard to predict
    actual_config = actual_args.pop()
    expected_config = expected_args.pop()
    assert ast.literal_eval(actual_config) == ast.literal_eval(expected_config)
    assert actual_args == expected_args
    assert jt.jobEnvironment == {"LC_ALL": "en_US.UTF-8", "LANG": "en_US.UTF-8"}
    expected_nativeSpecification = (
        f"-J {jt.jobName} "
        f"-A {CLI_KWARGS['account']} "
        f"-p {CLI_KWARGS['partition']} "
        f"--mem={CLI_KWARGS['peak_memory']*1024} "
        f"-t {CLI_KWARGS['max_runtime']}:00:00 "
        f"-c {CLI_KWARGS['num_threads']}"
    )
    assert jt.nativeSpecification == expected_nativeSpecification
