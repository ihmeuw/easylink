# mypy: ignore-errors
import pytest

from easylink.pipeline_schema import PipelineSchema
from easylink.pipeline_schema_constants import TESTING_SCHEMA_PARAMS
from easylink.runner import main
from tests.conftest import SPECIFICATIONS_DIR


@pytest.mark.slow
def test_missing_results(test_specific_results_dir, mocker, caplog):
    """Test that the pipeline fails when a step is missing output files."""
    nodes, edges = TESTING_SCHEMA_PARAMS["integration"]
    mocker.patch("easylink.pipeline_schema.ALLOWED_SCHEMA_PARAMS", TESTING_SCHEMA_PARAMS)
    mocker.patch(
        "easylink.configuration.Config._get_schema",
        return_value=PipelineSchema("integration", nodes=nodes, edges=edges),
    )

    ## Mock implementation script call to wait 1s instead of running something
    mocker.patch(
        "easylink.implementation.Implementation.script_cmd",
        new_callable=mocker.PropertyMock,
        return_value="sleep 1s",
    )
    with pytest.raises(SystemExit) as exit:
        main(
            command="run",
            pipeline_specification=SPECIFICATIONS_DIR / "integration" / "pipeline.yaml",
            input_data=SPECIFICATIONS_DIR / "common/input_data.yaml",
            computing_environment=SPECIFICATIONS_DIR / "common/environment_local.yaml",
            results_dir=test_specific_results_dir,
        )
    assert exit.value.code == 1
    assert "MissingOutputException" in caplog.text


@pytest.mark.slow
def test_outputting_a_directory(test_specific_results_dir, mocker):
    """Test that the pipeline fails when a step is missing output files."""
    nodes, edges = TESTING_SCHEMA_PARAMS["output_dir"]
    mocker.patch("easylink.pipeline_schema.ALLOWED_SCHEMA_PARAMS", TESTING_SCHEMA_PARAMS)
    mocker.patch(
        "easylink.configuration.Config._get_schema",
        return_value=PipelineSchema("output_dir", nodes=nodes, edges=edges),
    )

    with pytest.raises(SystemExit) as exit:
        main(
            command="run",
            pipeline_specification=SPECIFICATIONS_DIR
            / "integration"
            / "pipeline_output_dir.yaml",
            input_data=SPECIFICATIONS_DIR / "common/input_data_one_file.yaml",
            computing_environment=SPECIFICATIONS_DIR / "common/environment_local.yaml",
            results_dir=test_specific_results_dir,
        )
    assert exit.value.code == 0
