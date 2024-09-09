import os
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from easylink.rule import ImplementedRule, InputValidationRule, Rule, TargetRule

RULE_STRINGS = {
    "target_rule": "rule_strings/target_rule.txt",
    "implemented_rule_local": "rule_strings/implemented_rule_local.txt",
    "implemented_rule_slurm": "rule_strings/implemented_rule_slurm.txt",
    "validation_rule": "rule_strings/validation_rule.txt",
}


def test_rule_interface():
    class TestRule(Rule):
        def _build_rule(self) -> str:
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
    with open(file_path) as expected_file:
        expected = expected_file.read()
    rulestring = rule._build_rule()
    rulestring_lines = rulestring.split("\n")
    expected_lines = expected.split("\n")
    assert len(rulestring_lines) == len(expected_lines)
    for i, expected_line in enumerate(expected_lines):
        assert rulestring_lines[i].strip() == expected_line.strip()


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
                "validator": lambda x: None,
            },
            "secondary": {
                "env_var": "DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
                "filepaths": ["bar"],
                "validator": lambda x: None,
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
    with open(file_path) as expected_file:
        expected = expected_file.read()
    rulestring = rule._build_rule()
    rulestring_lines = rulestring.split("\n")
    expected_lines = expected.split("\n")
    assert len(rulestring_lines) == len(expected_lines)
    for i, expected_line in enumerate(expected_lines):
        assert rulestring_lines[i].strip() == expected_line.strip()


def bar():
    pass


def test_validation_rule_build_rule():
    rule = InputValidationRule(
        name="foo", slot_name="foo_input", input=["foo", "bar"], output="baz", validator=bar
    )
    file_path = Path(os.path.dirname(__file__)) / RULE_STRINGS["validation_rule"]
    with open(file_path) as expected_file:
        expected = expected_file.read()
    rulestring = rule._build_rule()
    rulestring_lines = rulestring.split("\n")
    expected_lines = expected.split("\n")
    assert len(rulestring_lines) == len(expected_lines)
    for i, expected_line in enumerate(expected_lines):
        assert rulestring_lines[i].strip() == expected_line.strip()
