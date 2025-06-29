import re
import subprocess
from pathlib import Path

import pytest
from pytest_mock import MockerFixture

from easylink.pipeline_schema import PipelineSchema
from easylink.pipeline_schema_constants import SCHEMA_PARAMS
from easylink.runner import main
from easylink.utilities.general_utils import is_on_slurm
from easylink.utilities.paths import DEV_IMAGES_DIR
from tests.conftest import SPECIFICATIONS_DIR


@pytest.mark.slow
@pytest.mark.skipif(
    not is_on_slurm(),
    reason="Must be on slurm to run this test.",
)
def test_slurm(
    test_specific_results_dir: Path, mocker: MockerFixture, caplog: pytest.LogCaptureFixture
) -> None:
    """Test that the pipeline runs on SLURM with appropriate resources."""
    nodes, edges = SCHEMA_PARAMS["integration"]
    mocker.patch(
        "easylink.configuration.Config._get_schema",
        return_value=PipelineSchema("integration", nodes=nodes, edges=edges),
    )
    with pytest.raises(SystemExit) as exit:
        main(
            command="run",
            pipeline_specification=SPECIFICATIONS_DIR / "integration/pipeline.yaml",
            input_data=SPECIFICATIONS_DIR / "common/input_data.yaml",
            computing_environment=SPECIFICATIONS_DIR
            / "integration/environment_spark_slurm.yaml",
            results_dir=test_specific_results_dir,
            images_dir=DEV_IMAGES_DIR,
            debug=True,
        )
    assert exit.value.code == 0
    output = caplog.text
    job_ids = re.findall(r"Job \d+ has been submitted with SLURM jobid (\d+)", output)
    assert len(job_ids) == 1
    cmd = [
        "sacct",
        "--jobs=" + ",".join(job_ids),
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
    assert account == "proj_simscience"
    assert partition == "all.q"
    assert mem == "1G" or mem == "1024M"  # Just in case
    assert cpus == "1"
    assert time == "60"
