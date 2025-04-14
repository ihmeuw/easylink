from pathlib import Path

import pytest

from easylink.pipeline_schema import PipelineSchema
from easylink.pipeline_schema_constants import TESTING_SCHEMA_PARAMS
from easylink.runner import main
from tests.conftest import SPECIFICATIONS_DIR

COMMON_SPECIFICATIONS_DIR = SPECIFICATIONS_DIR / "common"
EP_SPECIFICATIONS_DIR = SPECIFICATIONS_DIR / "integration" / "embarrassingly_parallel"


@pytest.mark.slow
def test_looping_embarrassingly_parallel_step(test_specific_results_dir: Path) -> None:
    pipeline_specification = EP_SPECIFICATIONS_DIR / "pipeline_loop_step.yaml"
    input_data = COMMON_SPECIFICATIONS_DIR / "input_data.yaml"

    # Load the schema to test against
    schema = PipelineSchema("looping_ep_step", *TESTING_SCHEMA_PARAMS["looping_ep_step"])

    # Run the pipeline. Snakemake will exit at the end so we need to catch that here
    _run_pipeline(schema, test_specific_results_dir, pipeline_specification, input_data)

    for loop in [1, 2]:
        implementation = f"step_1_loop_{loop}_step_1_python_pandas"
        implementation_subdir = test_specific_results_dir / "intermediate" / implementation
        # ensure that the input was split into two
        assert (
            len(list((implementation_subdir / "input_chunks").rglob("result.parquet"))) == 2
        )
        # ensure that each chunk was processed individually
        assert len(list((implementation_subdir / "processed").rglob("result.parquet"))) == 2
        # check for aggregated file
        assert (implementation_subdir / "result.parquet").exists()


####################
# Helper Functions #
####################


def _run_pipeline(
    schema: PipelineSchema,
    results_dir: Path,
    pipeline_specification: Path,
    input_data: Path,
    computing_environment: Path | None = None,
) -> None:
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        main(
            command="run",
            pipeline_specification=pipeline_specification,
            input_data=input_data,
            computing_environment=computing_environment,
            results_dir=results_dir,
            potential_schemas=schema,
        )
    assert pytest_wrapped_e.value.code == 0

    assert (results_dir / "result.parquet").exists()
    assert (results_dir / pipeline_specification.name).exists()
    assert (results_dir / input_data.name).exists()
    if computing_environment:
        assert (results_dir / computing_environment.name).exists()
