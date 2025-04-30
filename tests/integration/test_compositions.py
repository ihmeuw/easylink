from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import pytest

from easylink.pipeline_schema import PipelineSchema
from easylink.pipeline_schema_constants import TESTING_SCHEMA_PARAMS
from easylink.runner import main
from easylink.utilities.data_utils import load_yaml
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
    _run_pipeline_and_confirm_finished(
        schema, test_specific_results_dir, pipeline_specification, input_data
    )

    intermediate_results_dir = test_specific_results_dir / "intermediate"
    for loop in [1, 2]:
        step_name = f"step_1_loop_{loop}"
        # ensure that the input was split into two
        assert (
            len(
                list(
                    (intermediate_results_dir / f"{step_name}_step_1_main_input_split").rglob(
                        "result.parquet"
                    )
                )
            )
            == 2
        )
        # ensure that each chunk was processed individually
        assert (
            len(
                list(
                    (intermediate_results_dir / f"{step_name}_step_1_python_pandas").rglob(
                        "result.parquet"
                    )
                )
            )
            == 2
        )
        # check for aggregated file
        assert (
            intermediate_results_dir / f"{step_name}_aggregate" / "result.parquet"
        ).exists()


EP_SECTION_MAPPING = {
    "parallel_step": {
        "pipeline_spec": "pipeline_parallel_step.yaml",
        "schema_name": "ep_parallel_step",
        "node_prefix": "parallel_split",
    },
    "loop_step": {
        "pipeline_spec": "pipeline_loop_step.yaml",
        "schema_name": "ep_loop_step",
        "node_prefix": "loop",
    },
}


@pytest.mark.slow
@pytest.mark.parametrize(
    "step_type, expected_counter, num_rows_multiplier",
    [
        (
            "loop_step",
            2,
            1,
        ),
        (
            "parallel_step",
            # We have two mutually exclusive and collectively exhaustive subsets of
            # input data from the EmbarrassinglyParallelStep, each of which
            # is duplicated twice (from the ParallelStep). The step_1 container
            # is then run exactly one time on each of these four dataset.
            1,
            # The embarrassingly parallel splitting shouldn't increase the number of rows
            # in and of itself, but the underlying parallel step does. We have two
            # splits and so expect there to be twice as many rows in the final result.
            2,
        ),
    ],
)
def test_embarrassingly_parallel_sections(
    step_type: str,
    expected_counter: int,
    num_rows_multiplier: int,
    test_specific_results_dir: Path,
) -> None:
    # unpack the mapping
    pipeline_spec = EP_SECTION_MAPPING[step_type]["pipeline_spec"]
    schema_name = EP_SECTION_MAPPING[step_type]["schema_name"]
    node_prefix = EP_SECTION_MAPPING[step_type]["node_prefix"]

    pipeline_specification = EP_SPECIFICATIONS_DIR / pipeline_spec
    input_data = COMMON_SPECIFICATIONS_DIR / "input_data.yaml"

    # Load the schema to test against
    schema = PipelineSchema(schema_name, *TESTING_SCHEMA_PARAMS[schema_name])

    # Run the pipeline. Snakemake will exit at the end so we need to catch that here
    _run_pipeline_and_confirm_finished(
        schema, test_specific_results_dir, pipeline_specification, input_data
    )

    intermediate_results_dir = test_specific_results_dir / "intermediate"
    # ensure that the input was split into two
    assert (
        len(
            list(
                (intermediate_results_dir / "step_1_step_1_main_input_split").rglob(
                    "result.parquet"
                )
            )
        )
        == 2
    )
    for increment in [1, 2]:
        step_name = f"step_1_{node_prefix}_{increment}"
        # ensure that each chunk was processed individually
        assert (
            len(
                list(
                    (intermediate_results_dir / f"{step_name}_step_1_python_pandas").rglob(
                        "result.parquet"
                    )
                )
            )
            == 2
        )
    # check for aggregated file
    aggregated_filepath = intermediate_results_dir / "step_1_aggregate" / "result.parquet"
    assert aggregated_filepath.exists()
    # check that aggregated file was passed along correctly (i.e. made it to the next
    # step, which for this schema is the output step)
    df_aggregated = pd.read_parquet(aggregated_filepath)
    df_final = pd.read_parquet(test_specific_results_dir / "result.parquet")
    assert df_aggregated.equals(df_final)
    # check that the aggregated file is the expected shape
    input_num_rows = 0
    input_filepaths = load_yaml(input_data)
    for filepath in input_filepaths.values():
        input_num_rows += pq.read_metadata(filepath).num_rows
    assert len(df_final) == input_num_rows * num_rows_multiplier
    assert all(df_final["counter"] == expected_counter)
    assert [column for column in df_final.columns if column.startswith("added_column")] == [
        "added_column_0"
    ] + [f"added_column_{i+1}" for i in range(expected_counter)]


@pytest.mark.skip(reason="TODO [MIC-5979]")
@pytest.mark.slow
def test_embarrassingly_parallel_hierarchical_step(test_specific_results_dir: Path) -> None:
    ...


####################
# Helper Functions #
####################


def _run_pipeline_and_confirm_finished(
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
