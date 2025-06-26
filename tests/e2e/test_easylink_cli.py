# mypy: ignore-errors
import hashlib
import subprocess
import sys
from pathlib import Path
from pprint import pprint

import pytest

from easylink.utilities.data_utils import load_yaml
from easylink.utilities.general_utils import is_on_slurm
from easylink.utilities.paths import DEV_IMAGES_DIR
from tests.conftest import SPECIFICATIONS_DIR

RESULT_CHECKSUM = "d56f3d12c0c9ca89b6ce0f42810bb79cd16772535f73807722e5430819b6bc08"


@pytest.mark.slow
@pytest.mark.skipif(
    not is_on_slurm(),
    reason="Must be on slurm to run this test.",
)
@pytest.mark.parametrize(
    "pipeline_specification, input_data, computing_environment",
    [
        # local
        (
            "e2e/pipeline.yaml",
            "common/input_data.yaml",
            "common/environment_local.yaml",
        ),
        # slurm
        (
            "e2e/pipeline.yaml",
            "common/input_data.yaml",
            "e2e/environment_slurm.yaml",
        ),
    ],
)
def test_easylink_run(
    pipeline_specification,
    input_data,
    computing_environment,
    test_specific_results_dir,
    capsys,
):
    """Tests the 'easylink run' command

    Notes
    -----
    We use various print statements in this test because they show up in the
    Jenkins logs.
    """

    with capsys.disabled():  # disabled so we can monitor job submissions
        print(
            "\n\n*** RUNNING TEST ***\n"
            f"[{pipeline_specification}, {input_data}, {computing_environment}]\n"
        )

        cmd = (
            "easylink run "
            f"-p {SPECIFICATIONS_DIR / pipeline_specification} "
            f"-i {SPECIFICATIONS_DIR / input_data} "
            f"-e {SPECIFICATIONS_DIR / computing_environment} "
            f"-o {str(test_specific_results_dir)} "
            f"-I {DEV_IMAGES_DIR} "
            "--no-timestamp "
            "--schema development "
        )
        print(f"Running command: \n{cmd}\n")
        subprocess.run(
            cmd,
            shell=True,
            stdout=sys.stdout,
            stderr=sys.stderr,
            check=True,
        )
        final_output = test_specific_results_dir / "result.parquet"
        assert final_output.exists()
        # Check that the results file checksum matches the expected value
        with open(final_output, "rb") as f:
            actual_checksum = hashlib.sha256(f.read()).hexdigest()
        assert actual_checksum == RESULT_CHECKSUM

        assert (test_specific_results_dir / Path(pipeline_specification).name).exists()
        assert (test_specific_results_dir / Path(input_data).name).exists()
        assert (test_specific_results_dir / Path(computing_environment).name).exists()

        # Check that implementation configuration worked
        diagnostics_dir = test_specific_results_dir / "diagnostics"
        assert (
            load_yaml(diagnostics_dir / "step_1_python_pandas" / "diagnostics.yaml")[
                "increment"
            ]
            == 1
        )
        assert (
            load_yaml(diagnostics_dir / "step_2_python_pyspark" / "diagnostics.yaml")[
                "increment"
            ]
            == 100
        )
        assert (
            load_yaml(diagnostics_dir / "step_3_python_pandas" / "diagnostics.yaml")[
                "increment"
            ]
            == 702
        )
        assert (
            load_yaml(diagnostics_dir / "step_4_r" / "diagnostics.yaml")["increment"] == 912
        )
        # If it made it through all this, print some diagnostics and delete the results_dir
        final_diagnostics = load_yaml(
            sorted([d for d in diagnostics_dir.iterdir() if d.is_dir()])[-1]
            / "diagnostics.yaml"
        )
        print("\nFinal diagnostics:\n")
        pprint(final_diagnostics)
        print(
            "\n\n*** END OF TEST ***\n"
            f"[{pipeline_specification}, {input_data}, {computing_environment}]\n"
        )


@pytest.mark.skipif(
    not is_on_slurm(),
    reason="Must be on slurm to run this test.",
)
def test_easylink_generate_dag(test_specific_results_dir):
    cmd = (
        "easylink generate-dag "
        f"-p {SPECIFICATIONS_DIR / 'e2e' / 'pipeline.yaml'} "
        f"-i {SPECIFICATIONS_DIR / 'common' / 'input_data.yaml'} "
        f"-o {str(test_specific_results_dir)} "
        "--no-timestamp "
        "--schema development "
    )
    subprocess.run(
        cmd,
        shell=True,
        stdout=sys.stdout,
        stderr=sys.stderr,
        check=True,
    )
    assert (test_specific_results_dir / "DAG.svg").exists()
