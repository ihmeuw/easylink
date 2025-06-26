# mypy: ignore-errors
import os
import shlex
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from easylink.rule import (
    AggregationRule,
    CheckpointRule,
    ImplementedRule,
    InputValidationRule,
    Rule,
    TargetRule,
)

RULE_STRINGS = {
    "target_rule": "rule_strings/target_rule.txt",
    "implemented_rule_local": "rule_strings/implemented_rule_local.txt",
    "implemented_rule_slurm": "rule_strings/implemented_rule_slurm.txt",
    "validation_rule": "rule_strings/validation_rule.txt",
    "auto_parallel_rule": "rule_strings/auto_parallel_rule.txt",
    "checkpoint_rule": "rule_strings/checkpoint_rule.txt",
    "aggregation_rule": "rule_strings/aggregation_rule.txt",
}


def test_rule_interface():
    class TestRule(Rule):
        def build_rule(self) -> str:
            return "'Tis but a scratch!"

    rule = TestRule()
    with TemporaryDirectory() as snake_dir_path:
        snakefile_path = Path(snake_dir_path) / "Snakefile"
        rule.write_to_snakefile(snakefile_path)
        with open(snakefile_path, "r") as f:
            assert f.read() == "'Tis but a scratch!"


def test_target_rule_build_rule():
    rule = TargetRule(
        target_files=["int/foo", "int/bar"],
        validation="bar",
        requires_spark=False,
    )
    file_path = Path(os.path.dirname(__file__)) / RULE_STRINGS["target_rule"]
    _check_rule(rule, file_path)


@pytest.mark.parametrize("computing_environment", ["local", "slurm"])
@pytest.mark.parametrize(
    "output",
    [
        ["some/file.txt"],
        ["some/file.txt", "some/other/file.txt"],
        ["some/directory"],
        ["some/directory", "some/other/directory"],
        ["some/file.txt", "some/directory"],
    ],
)
def test_implemented_rule_build_rule(
    computing_environment: str, output: list[str], tmp_path: Path
) -> None:
    if computing_environment == "slurm":
        resources = {
            "slurm_partition": "'slurmpart'",
            "runtime": 1,
            "mem_mb": 5120,
            "cpus_per_task": 1337,
        }
    else:
        resources = None

    if output == ["some/directory", "some/other/directory"]:
        pytest.xfail(
            reason="NotImplementedError: Multiple output directories is not supported."
        )
    if output == ["some/file.txt", "some/directory"]:
        pytest.xfail(
            reason="NotImplementedError: Mixed output types (files and directories) is not supported."
        )

    # The output in the snakefile will depend on whether the output is a file or a directory.
    file_path = (
        Path(os.path.dirname(__file__))
        / RULE_STRINGS[f"implemented_rule_{computing_environment}"]
    )
    with open(file_path) as expected_file:
        expected_str = expected_file.read()

    if output == ["some/file.txt"]:
        pass  # the epxected file is already correct
    elif output == ["some/file.txt", "some/other/file.txt"]:
        expected_str = expected_str.replace("output: ['some/file.txt']", f"output: {output}")
        expected_str = expected_str.replace(
            "export OUTPUT_PATHS=some/file.txt",
            "export OUTPUT_PATHS=some/file.txt,some/other/file.txt",
        )
    elif output == ["some/directory"]:
        expected_str = expected_str.replace(
            "output: ['some/file.txt']", "output: directory('some/directory')"
        )
        expected_str = expected_str.replace(
            "export OUTPUT_PATHS=some/file.txt",
            "export OUTPUT_PATHS=some/directory",
        )
    else:
        raise NotImplementedError
    # move the modified snakefile to tmpdir
    expected_filepath = tmp_path / file_path.name
    with open(expected_filepath, "w") as expected_file:
        expected_file.write(expected_str)

    rule = ImplementedRule(
        name="foo_rule",
        step_name="foo_step",
        implementation_name="foo_imp",
        input_slots={
            "main": {
                "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                "filepaths": ["foo"],
                "validator": _dummy_validator,
            },
            "secondary": {
                "env_var": "DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
                "filepaths": ["bar"],
                "validator": _dummy_validator,
            },
        },
        validations=["bar"],
        output=output,
        resources=resources,
        envvars={"eggs": "coconut"},
        diagnostics_dir="spam",
        image_path="Multipolarity.sif",
        script_cmd="echo hello world",
        requires_spark=False,
    )

    _check_rule(rule, expected_filepath)


def test_auto_parallel_rule_build_rule():
    rule = ImplementedRule(
        name="foo_rule",
        step_name="foo_step",
        implementation_name="foo_imp",
        input_slots={
            "main": {
                "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                "filepaths": ["some/path/to/input_chunks/{chunk}/result.parquet"],
                "validator": _dummy_validator,
                "splitter": _dummy_splitter,
            },
            "secondary": {
                "env_var": "DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
                "filepaths": ["bar"],
                "validator": _dummy_validator,
                "splitter": None,
            },
        },
        validations=["baz"],
        output=["some/path/to/processed/{chunk}/result.parquet"],
        resources=None,
        envvars={"eggs": "coconut"},
        diagnostics_dir="spam",
        image_path="Multipolarity.sif",
        script_cmd="echo hello world",
        requires_spark=False,
        is_auto_parallel=True,
    )

    file_path = Path(os.path.dirname(__file__)) / RULE_STRINGS["auto_parallel_rule"]
    _check_rule(rule, file_path)


def test_auto_parallel_rule_build_rule_multiple_outputs_raises():
    """Temporary test against raising if an auto-parallel step has multiple outputs."""
    rule = ImplementedRule(
        name="foo_rule",
        step_name="foo_step",
        implementation_name="foo_imp",
        input_slots={
            "main": {
                "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                "filepaths": ["foo"],
                "validator": _dummy_validator,
                "splitter": _dummy_splitter,
            },
        },
        validations=["baz"],
        output=["some/path/to/quux", "some/other/path/to/something"],
        resources=None,
        envvars={"eggs": "coconut"},
        diagnostics_dir="spam",
        image_path="Multipolarity.sif",
        script_cmd="echo hello world",
        requires_spark=False,
        is_auto_parallel=True,
    )

    with pytest.raises(
        NotImplementedError,
        match="Multiple output slots/files of AutoParallelSteps not yet supported",
    ):
        rule.build_rule()


def test_checkpoint_rule_build_rule():
    rule = CheckpointRule(
        name="this_is_a_checkpoint_rule",
        input_files=["some/input/file1", "some/input/file2"],
        splitter_func_name="my_splitter",
        output_dir="this/is/the/output/dir",
        checkpoint_filepath="this/is/the/checkpoint.txt",
    )

    file_path = Path(os.path.dirname(__file__)) / RULE_STRINGS["checkpoint_rule"]
    _check_rule(rule, file_path)


def test_aggregation_rule_build_rule():
    rule = AggregationRule(
        name="this_is_an_aggregation_rule",
        input_files=["these/are/processed/{chunk}/result.parquet"],
        aggregated_output_file="some/path/to/aggregated/results/result.parquet",
        aggregator_func_name="my_aggregator",
        checkpoint_filepath="this/is/the/checkpoint.txt",
        checkpoint_rule_name="checkpoints.this_is_a_checkpoint_rule",
    )

    file_path = Path(os.path.dirname(__file__)) / RULE_STRINGS["aggregation_rule"]
    _check_rule(rule, file_path)


def test_validation_rule_build_rule():
    rule = InputValidationRule(
        name="foo",
        input_slot_name="foo_input",
        input=["foo", "bar"],
        output="baz",
        validator=_dummy_validator,
    )
    file_path = Path(os.path.dirname(__file__)) / RULE_STRINGS["validation_rule"]
    _check_rule(rule, file_path)


@pytest.mark.parametrize(
    "name, value, expected",
    [
        # Basic cases
        ("SIMPLE_VAR", "simple_value", "export SIMPLE_VAR=simple_value"),
        (
            "VAR_WITH_SPACES",
            "value with spaces",
            "export VAR_WITH_SPACES='value with spaces'",
        ),
        # Quote cases
        (
            "VAR_WITH_SINGLE_QUOTES",
            "value with 'single quotes'",
            "export VAR_WITH_SINGLE_QUOTES='value with '\"'\"'single quotes'\"'\"''",
        ),
        (
            "VAR_WITH_DOUBLE_QUOTES",
            'with "double" quotes',
            "export VAR_WITH_DOUBLE_QUOTES='with \"double\" quotes'",
        ),
        (
            "VAR_WITH_MIXED_QUOTES",
            "with both \"double\" and 'single' quotes",
            "export VAR_WITH_MIXED_QUOTES='with both \"double\" and '\"'\"'single'\"'\"' quotes'",
        ),
        # Complex structured data
        (
            "VAR_WITH_CHARS",
            "col1='value1',col2='value2'",
            "export VAR_WITH_CHARS='col1='\"'\"'value1'\"'\"',col2='\"'\"'value2'\"'\"''",
        ),
        (
            "COMPLEX_VAR",
            "l.first_name == r.first_name,l.last_name == r.last_name,col1='value1',col2='value2'",
            "export COMPLEX_VAR='l.first_name == r.first_name,l.last_name == r.last_name,col1='\"'\"'value1'\"'\"',col2='\"'\"'value2'\"'\"''",
        ),
        # Edge cases that could cause shell injection or parsing issues
        ("EMPTY_VAR", "", "export EMPTY_VAR=''"),
        (
            "SHELL_METACHAR_VAR",
            "with$dollar`backtick",
            "export SHELL_METACHAR_VAR='with$dollar`backtick'",
        ),
        (
            "COMMAND_SEP_VAR",
            "with;semicolon&ampersand",
            "export COMMAND_SEP_VAR='with;semicolon&ampersand'",
        ),
        (
            "PIPE_REDIRECT_VAR",
            "with|pipe>redirect",
            "export PIPE_REDIRECT_VAR='with|pipe>redirect'",
        ),
        (
            "WHITESPACE_VAR",
            "with\nnewline\ttab",
            "export WHITESPACE_VAR='with\nnewline\ttab'",
        ),
        # Original bug cases
        (
            "BLOCKING_RULES",
            "l.last_name == r.last_name",
            "export BLOCKING_RULES='l.last_name == r.last_name'",
        ),
        (
            "BLOCKING_RULES_FOR_TRAINING",
            "l.first_name == r.first_name,l.last_name == r.last_name",
            "export BLOCKING_RULES_FOR_TRAINING='l.first_name == r.first_name,l.last_name == r.last_name'",
        ),
    ],
)
def test_implemented_rule_envvar_quoting(name, value, expected):
    """Test that environment variables with special characters are properly quoted in shell commands."""

    rule = ImplementedRule(
        name="test_rule",
        step_name="test_step",
        implementation_name="test_impl",
        input_slots={},
        validations=[],
        output=["test_output.txt"],
        resources=None,
        envvars={name: value},
        diagnostics_dir="test_diagnostics",
        image_path="test.sif",
        script_cmd="echo test",
        requires_spark=False,
    )

    shell_cmd = rule._build_shell_cmd()

    # Verify that each environment variable is properly quoted using shlex.quote
    assert expected == f"export {name}={shlex.quote(str(value))}"
    assert expected in shell_cmd


####################
# Helper functions #
####################


def _check_rule(rule: Rule, expected_rule_path: str):
    """Compares the ``Rule's`` built rule to the expected rule in the file."""
    with open(expected_rule_path) as expected_file:
        expected = expected_file.read()
    rulestring = rule.build_rule()
    rulestring_lines = rulestring.split("\n")
    expected_lines = expected.split("\n")
    assert len(rulestring_lines) == len(expected_lines)
    for i, expected_line in enumerate(expected_lines):
        assert rulestring_lines[i].strip() == expected_line.strip()


def _dummy_validator():
    """Dummy validator function"""
    pass


def _dummy_splitter():
    """Dummy splitter function"""
    pass


def _dummy_aggregator():
    """Dummy aggregator function"""
    pass
