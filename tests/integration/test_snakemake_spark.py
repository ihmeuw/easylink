import re
import subprocess
import tempfile
from pathlib import Path

import pytest

from linker.pipeline_schema import PipelineSchema, validate_dummy_input
from linker.runner import main
from linker.step import Step
from linker.utilities.general_utils import is_on_slurm
from tests.conftest import SPECIFICATIONS_DIR

JOB_TYPE = {0: "spark", 1: "spark", 2: "implementation"}
RESOURCES = {
    "spark": {
    "account": "proj_simscience",
    "partition": "all.q",
    "mem": "1G",
    "cpus": "1",
    "time": "60", },
    "implementation": {
    "account": "proj_simscience",
    "partition": "all.q",
    "mem": "1G",
    "cpus": "1",
    "time": "60", }
}


@pytest.mark.slow
@pytest.mark.skipif(
    not is_on_slurm(),
    reason="Must be on slurm to run this test.",
)
def test_spark_slurm(mocker, caplog):
    """Test that the pipeline runs spark on SLURM with appropriate resources."""
    mocker.patch(
        "linker.configuration.Config._get_schema",
        return_value=PipelineSchema._generate_schema(
            "test",
            validate_dummy_input,
            Step("step_1"),
        ),
    )
    with tempfile.TemporaryDirectory(dir="tests/integration/") as results_dir:
        results_dir = Path(results_dir).resolve()
        with pytest.raises(SystemExit) as exit:
            main(
                SPECIFICATIONS_DIR / "integration" / "pipeline_spark.yaml",
                SPECIFICATIONS_DIR / "input_data.yaml",
                SPECIFICATIONS_DIR / "environment_slurm.yaml",
                str(results_dir),
                debug=True,
            )
        assert exit.value.code == 0
        output = caplog.text
        job_ids = re.findall(r"Job \d+ has been submitted with SLURM jobid (\d+)", output)
        assert len(job_ids) == 3
        for job_id in job_ids:
            cmd = [
                "sacct",
                "--jobs=" + ",".join(job_id),
                "--format=JobID,Account,Partition,ReqMem,ReqCPUS,TimelimitRaw",
                "--noheader",
                "--parsable2",
            ]
            result = subprocess.run(cmd, capture_output=True, text=True)
            output = result.stdout
            # Filter out jobs that are not the main job and grab full line
            main_job_pattern = r"^(\d+)\|"
            main_job_lines = [
                line for line in output.split("\n") if re.match(main_job_pattern, line)
            ]
            for line in main_job_lines:
                fields = line.split("|")
                account, partition, mem, cpus, time = fields[1:6]
                assert account == RESOURCES[JOB_TYPE[job_id]]["account"]
                assert partition == RESOURCES[JOB_TYPE[job_id]]["partition"]
                assert mem == RESOURCES[JOB_TYPE[job_id]]["mem"]
                assert cpus == RESOURCES[JOB_TYPE[job_id]]["cpus"]
                assert time == RESOURCES[JOB_TYPE[job_id]]["time"]