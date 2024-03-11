import os
from pathlib import Path
from tempfile import TemporaryDirectory

from linker.rule import ImplementedRule, InputValidationRule, Rule, TargetRule

RULE_STRINGS = {
    "target_rule": "rule_strings/target_rule.txt",
    "implemented_rule": "rule_strings/implemented_rule.txt",
    "validation_rule": "rule_strings/validation_rule.txt",
}


def test_rule_interface():
    class TestRule(Rule):
        def _build_rule(self) -> str:
            return "'Tis but a scratch!"

    rule = TestRule()
    with TemporaryDirectory() as snake_dir_path:
        snake_dir = Path(snake_dir_path)
        rule.write_to_snakefile(snake_dir)
        with open(snake_dir / "Snakefile", "r") as f:
            assert f.read() == "'Tis but a scratch!"


def test_target_rule_build_rule():
    rule = TargetRule(
        target_files=["foo", "bar"],
        validation="bar",
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


def test_implemented_rule_build_rule():
    rule = ImplementedRule(
        name="foo",
        execution_input=["foo", "bar"],
        validation="bar",
        output=["baz"],
        envvars={"eggs": "coconut"},
        diagnostics_dir="spam",
        script_cmd="echo hello world",
    )
    file_path = Path(os.path.dirname(__file__)) / RULE_STRINGS["implemented_rule"]
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
    rule = InputValidationRule(name="foo", input=["foo", "bar"], output="baz", validator=bar)
    file_path = Path(os.path.dirname(__file__)) / RULE_STRINGS["validation_rule"]
    with open(file_path) as expected_file:
        expected = expected_file.read()
    rulestring = rule._build_rule()
    rulestring_lines = rulestring.split("\n")
    expected_lines = expected.split("\n")
    assert len(rulestring_lines) == len(expected_lines)
    for i, expected_line in enumerate(expected_lines):
        assert rulestring_lines[i].strip() == expected_line.strip()
