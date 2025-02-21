# mypy: ignore-errors
import os
import re
import subprocess
import tempfile

import pytest
import yaml

from easylink.pipeline_schema import PipelineSchema
from easylink.pipeline_schema_constants import TESTING_SCHEMA_PARAMS
from easylink.runner import main
from easylink.utilities.general_utils import is_on_slurm
from tests.conftest import RESULTS_DIR, SPECIFICATIONS_DIR


@pytest.mark.slow
@pytest.mark.skipif(
    not is_on_slurm(),
    reason="Must be on slurm to run this test.",
)
def test_spark_slurm(mocker, caplog):
    """Test that the pipeline runs spark on SLURM with appropriate resources."""
    nodes, edges = TESTING_SCHEMA_PARAMS["integration"]
    mocker.patch("easylink.pipeline_schema.ALLOWED_SCHEMA_PARAMS", TESTING_SCHEMA_PARAMS)
    mocker.patch(
        "easylink.configuration.Config._get_schema",
        return_value=PipelineSchema("integration", nodes=nodes, edges=edges),
    )
    results_dir = tempfile.mkdtemp(dir=RESULTS_DIR)
    # give the tmpdir the same permissions as the parent directory so that
    # cluster jobs can write to it
    os.chmod(results_dir, os.stat(RESULTS_DIR).st_mode)
    pipeline_specification = SPECIFICATIONS_DIR / "integration/pipeline_spark.yaml"
    input_data = SPECIFICATIONS_DIR / "common/input_data.yaml"
    computing_environment = SPECIFICATIONS_DIR / "integration/environment_spark_slurm.yaml"
    with open(computing_environment, "r") as stream:
        env_config = yaml.safe_load(stream)
    with pytest.raises(SystemExit) as exit:
        main(
            command="run",
            pipeline_specification=pipeline_specification,
            input_data=input_data,
            computing_environment=computing_environment,
            results_dir=str(results_dir),
            debug=True,
        )
    assert exit.value.code == 0
    output = caplog.text
    job_ids = re.findall(r"Job \d+ has been submitted with SLURM jobid (\d+)", output)
    assert len(job_ids) == 3
    for idx, job_id in enumerate(job_ids):
        cmd = [
            "sacct",
            f"--jobs={job_id}",
            "--format=JobID,Account,Partition,ReqMem,ReqCPUS,TimelimitRaw",
            "--noheader",
            "--parsable2",
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        output = result.stdout
        # Filter out jobs that are not the main job and grab full line
        main_job_pattern = r"^(\d+)\|"
        main_line = None
        for line in output.split("\n"):
            if re.match(main_job_pattern, line):
                main_line = line
                break
        assert main_line
        fields = main_line.split("|")
        account, partition, mem, cpus, time = fields[1:6]
        assert account == env_config["slurm"]["account"]
        assert partition == env_config["slurm"]["partition"]
        # First two jobs are spark master and spark worker
        if idx in [0, 1]:
            assert (
                mem
                == str(int(env_config["spark"]["workers"]["mem_per_node"] * 1024 + 500)) + "M"
            )
            assert cpus == str(env_config["spark"]["workers"]["cpus_per_node"])
            assert time == str(int(env_config["spark"]["workers"]["time_limit"] * 60))
        else:
            assert mem == str(env_config["implementation_resources"]["memory"]) + "G"
            assert cpus == str(env_config["implementation_resources"]["cpus"])
            assert time == str(int(env_config["implementation_resources"]["time_limit"] * 60))
