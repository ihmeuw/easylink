# mypy: ignore-errors
import subprocess
import sys

import pytest

from easylink.utilities.paths import DEV_IMAGES_DIR
from tests.conftest import SPECIFICATIONS_DIR


@pytest.mark.slow
@pytest.mark.parametrize(
    "pipeline_specification, implementations",
    [
        # Basic
        (
            "common/pipeline.yaml",
            [
                "step_1_python_pandas",
                "step_2_python_pandas",
                "step_3_python_pandas",
                "step_4_python_pandas",
            ],
        ),
        # Expanded pipeline with loops, clones, hierarchy
        (
            "e2e/pipeline_expanded.yaml",
            [
                "step_1_clone_1_step_1_python_pandas",
                "step_1_clone_2_step_1_python_pandas",
                "step_1_clone_3_step_1_python_pandas",
                "step_2_python_pandas",
                "step_3_loop_1_step_3_python_pandas",
                "step_3_loop_2_step_3_python_pandas",
                "step_4a_python_pandas",
                "step_4b_python_pandas",
            ],
        ),
    ],
)
def test_step_types(
    pipeline_specification, implementations, test_specific_results_dir, capsys
):
    """Tests against various permutations of complex step types.

    The goal is to test that EasyLink generates the correct output implementations
    depending on the configuration; i.e. if we have a 'substeps' key in the config,
    we get an implementation for each (or else we get a single implementation).
    """
    input_data = "common/input_data.yaml"
    computing_environment = "common/environment_local.yaml"
    with capsys.disabled():  # disabled so we can monitor job submissions
        print("\n\n*** STARTING RUN ***\n" f"[{pipeline_specification}]\n")

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
        print("\n\n*** END OF RUN ***\n" f"[{pipeline_specification}]\n")

    assert (test_specific_results_dir / "result.parquet").exists()

    # Check that we get directories for particular implementations
    for implementation in implementations:
        assert (test_specific_results_dir / "diagnostics" / implementation).exists()
        # step_3 is auto-parallel so the final results land in _aggregate/
        aggregate_mapping = {
            "step_3_python_pandas": "step_3_aggregate",
            "step_3_loop_1_step_3_python_pandas": "step_3_loop_1_aggregate",
            "step_3_loop_2_step_3_python_pandas": "step_3_loop_2_aggregate",
        }
        subdir = aggregate_mapping.get(implementation, implementation)
        assert (
            test_specific_results_dir / "intermediate" / subdir / "result.parquet"
        ).exists()
