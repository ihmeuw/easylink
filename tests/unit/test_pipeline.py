from pathlib import Path
from tempfile import TemporaryDirectory

from linker.pipeline import Pipeline


def test__get_implementations(default_config, mocker):
    mocker.patch("linker.implementation.Implementation.validate", return_value={})
    pipeline = Pipeline(default_config)
    implementation_names = [
        implementation.name for implementation in pipeline.implementations
    ]
    assert implementation_names == ["step_1_python_pandas", "step_2_python_pandas"]


def test_get_step_id(default_config, mocker):
    mocker.patch("linker.implementation.Implementation.validate", return_value={})
    pipeline = Pipeline(default_config)
    assert pipeline.get_step_id(pipeline.implementations[0]) == "1_step_1"
    assert pipeline.get_step_id(pipeline.implementations[1]) == "2_step_2"


def test_get_input_files(default_config, mocker, test_dir):
    mocker.patch("linker.implementation.Implementation.validate", return_value={})
    pipeline = Pipeline(default_config)
    assert pipeline.get_input_files(pipeline.implementations[0], Path("/tmp")) == [
        test_dir + "/input_data1/file1.csv",
        test_dir + "/input_data2/file2.csv",
    ]
    assert pipeline.get_input_files(pipeline.implementations[1], Path("/tmp")) == [
        "/tmp/intermediate/1_step_1/result.parquet"
    ]


def test_get_output_dir(default_config, mocker):
    mocker.patch("linker.implementation.Implementation.validate", return_value={})
    pipeline = Pipeline(default_config)
    assert pipeline.get_output_dir(pipeline.implementations[0], Path("/tmp")) == Path(
        "/tmp/intermediate/1_step_1"
    )
    assert pipeline.get_output_dir(pipeline.implementations[1], Path("/tmp")) == Path("/tmp")


def test_get_diagnostic_dir(default_config, mocker):
    mocker.patch("linker.implementation.Implementation.validate", return_value={})
    pipeline = Pipeline(default_config)
    assert pipeline.get_diagnostics_dir(pipeline.implementations[0], Path("/tmp")) == Path(
        "/tmp/diagnostics/1_step_1"
    )
    assert pipeline.get_diagnostics_dir(pipeline.implementations[1], Path("/tmp")) == Path(
        "/tmp/diagnostics/2_step_2"
    )


def test_build_snakefile(default_config, mocker, test_dir):
    mocker.patch("linker.implementation.Implementation.validate", return_value={})
    pipeline = Pipeline(default_config)
    with TemporaryDirectory() as snake_dir:
        snakefile = pipeline.build_snakefile(Path(snake_dir))
        expected = f"""import linker.utilities.validation_utils as validation_utils
rule all:
    input:
        final_output=['{snake_dir}/result.parquet'],
        validation='{snake_dir}/input_validations/final_validator'
rule:
    name: "results_validator"
    input: ['{snake_dir}/result.parquet']
    output: touch("{snake_dir}/input_validations/final_validator")
    localrule: True         
    message: "Validating results input"
    run:
        for f in input:
            validation_utils.validate_dummy_file(f)
rule:
    name: "step_1_python_pandas_validator"
    input: ['{test_dir}/input_data1/file1.csv', '{test_dir}/input_data2/file2.csv']
    output: touch("{snake_dir}/input_validations/step_1_python_pandas_validator")
    localrule: True         
    message: "Validating step_1_python_pandas input"
    run:
        for f in input:
            validation_utils.validate_dummy_file(f)
rule:
    name: "step_1_python_pandas"
    input:
        implementation_inputs=['{test_dir}/input_data1/file1.csv', '{test_dir}/input_data2/file2.csv'],
        validation="{snake_dir}/input_validations/step_1_python_pandas_validator"
    output: ['{snake_dir}/intermediate/1_step_1/result.parquet']
    shell:
        '''
        export DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS={test_dir}/input_data1/file1.csv,{test_dir}/input_data2/file2.csv
        export DUMMY_CONTAINER_OUTPUT_PATHS={snake_dir}/intermediate/1_step_1/result.parquet
        export DUMMY_CONTAINER_DIAGNOSTICS_DIRECTORY={snake_dir}/diagnostics/1_step_1
        python src/linker/steps/dev/python_pandas/dummy_step.py
        '''
rule:
    name: "step_2_python_pandas_validator"
    input: ['{snake_dir}/intermediate/1_step_1/result.parquet']
    output: touch("{snake_dir}/input_validations/step_2_python_pandas_validator")
    localrule: True         
    message: "Validating step_2_python_pandas input"
    run:
        for f in input:
            validation_utils.validate_dummy_file(f)
rule:
    name: "step_2_python_pandas"
    input:
        implementation_inputs=['{snake_dir}/intermediate/1_step_1/result.parquet'],
        validation="{snake_dir}/input_validations/step_2_python_pandas_validator"
    output: ['{snake_dir}/result.parquet']
    shell:
        '''
        export DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS={snake_dir}/intermediate/1_step_1/result.parquet
        export DUMMY_CONTAINER_OUTPUT_PATHS={snake_dir}/result.parquet
        export DUMMY_CONTAINER_DIAGNOSTICS_DIRECTORY={snake_dir}/diagnostics/2_step_2
        python src/linker/steps/dev/python_pandas/dummy_step.py
        '''
            """
        snake_str = snakefile.read_text()
        for test_line, expected_line in zip(snake_str.split("\n"), expected.split("\n")):
            assert test_line.strip() == expected_line.strip()
