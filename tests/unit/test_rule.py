# mypy: ignore-errors
import os
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
    "embarrassingly_parallel_rule": "rule_strings/embarrassingly_parallel_rule.txt",
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
def test_implemented_rule_build_rule(computing_environment):
    if computing_environment == "slurm":
        resources = {
            "slurm_partition": "'slurmpart'",
            "runtime": 1,
            "mem_mb": 5120,
            "cpus_per_task": 1337,
        }
    else:
        resources = None

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
        output=["baz"],
        resources=resources,
        envvars={"eggs": "coconut"},
        diagnostics_dir="spam",
        image_path="Multipolarity.sif",
        script_cmd="echo hello world",
        requires_spark=False,
    )

    file_path = (
        Path(os.path.dirname(__file__))
        / RULE_STRINGS[f"implemented_rule_{computing_environment}"]
    )
    _check_rule(rule, file_path)


def test_embarrassingly_parallel_rule_build_rule():
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
            "secondary": {
                "env_var": "DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
                "filepaths": ["bar"],
                "validator": _dummy_validator,
                "splitter": None,
            },
        },
        validations=["baz"],
        output=["some/path/to/quux"],
        resources=None,
        envvars={"eggs": "coconut"},
        diagnostics_dir="spam",
        image_path="Multipolarity.sif",
        script_cmd="echo hello world",
        requires_spark=False,
        is_embarrassingly_parallel=True,
    )

    file_path = Path(os.path.dirname(__file__)) / RULE_STRINGS["embarrassingly_parallel_rule"]
    _check_rule(rule, file_path)


def test_embarrassingly_parallel_rule_build_rule_multiple_outputs_raises():
    """Temporary test against raising if an embarrassingly parallel step has multiple outputs."""
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
        is_embarrassingly_parallel=True,
    )

    with pytest.raises(
        NotImplementedError,
        match="Multiple output slots/files of EmbarrassinglyParallelSteps not yet supported",
    ):
        rule.build_rule()


def test_checkpoint_build_rule():
    rule = CheckpointRule(
        name="foo_rule",
        input_slots={
            "main": {
                "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                "filepaths": ["foo"],
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
        output=["some/path/to/quux"],
    )

    file_path = Path(os.path.dirname(__file__)) / RULE_STRINGS["checkpoint_rule"]
    _check_rule(rule, file_path)


def test_aggregation_build_rule():
    rule = AggregationRule(
        name="foo_rule",
        input_slots={
            "main": {
                "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                "filepaths": ["foo"],
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
        output_slot_name="main_output",
        output_slot={
            "filepaths": ["some/path/to/quux"],
            "aggregator": _dummy_aggregator,
        },
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
