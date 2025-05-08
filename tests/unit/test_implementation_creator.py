import os
from pathlib import Path

import pytest

from easylink.devtools.implementation_creator import ImplementationCreator

GOOD_METADATA = """
# STEP_NAME: blocking
# REQUIREMENTS: pandas==2.1.2 pyarrow pyyaml"""

MISSING_METADATA = """
# step_name: blocking
# requirements: pandas==2.1.2 pyarrow pyyaml
// STEP_NAME: blocking
// REQUIREMENTS: pandas==2.1.2 pyarrow pyyaml
gosh I wish I'd capitalized the metadata keys or used the correct comment symbol!"""

MULTIPLE_METADATA = """
# STEP_NAME: blocking
# REQUIREMENTS: pandas==2.1.2 pyarrow pyyaml
# STEP_NAME: blocking
# REQUIREMENTS: pandas==2.1.2 pyarrow pyyaml"""


def test__extract_step_name(tmp_path: Path) -> None:
    script_path = tmp_path / "foo_step.py"
    with open(script_path, "w") as file:
        file.write(GOOD_METADATA)
    assert ImplementationCreator._extract_step_name(script_path) == "blocking"


@pytest.mark.parametrize(
    "script_content, error_msg",
    [
        (MISSING_METADATA, "Could not find a step name"),
        (MULTIPLE_METADATA, "Found multiple step_name requests"),
    ],
)
def test__extract_step_name_raises(
    script_content: str, error_msg: str, tmp_path: Path
) -> None:
    script_path = tmp_path / "foo_step.py"
    with open(script_path, "w") as file:
        file.write(script_content)
    with pytest.raises(ValueError, match=error_msg):
        ImplementationCreator._extract_step_name(script_path)


@pytest.mark.parametrize(
    "script_content, expected",
    [
        (GOOD_METADATA, "pandas==2.1.2 pyarrow pyyaml"),
        (MISSING_METADATA, ""),
    ],
)
def test__extract_requirements(script_content: str, expected: str, tmp_path: Path) -> None:
    script_path = tmp_path / "foo_step.py"
    with open(script_path, "w") as file:
        file.write(script_content)
    assert ImplementationCreator._extract_requirements(script_path) == expected


def test__extract_requirements_raises(tmp_path: Path) -> None:
    script_path = tmp_path / "foo_step.py"
    with open(script_path, "w") as file:
        file.write(MULTIPLE_METADATA)
    with pytest.raises(ValueError, match="Found multiple requirements requests"):
        ImplementationCreator._extract_requirements(script_path)


def test_write_recipe(tmp_path: Path) -> None:
    script_path = tmp_path / "cookies.py"
    with open(script_path, "w") as file:
        file.write(GOOD_METADATA)
    creator = ImplementationCreator(script_path)
    creator.create_recipe()
    expected_recipe_path = (
        Path(os.path.dirname(__file__)) / "recipe_strings" / "python_pandas.txt"
    )
    _check_recipe(script_path, expected_recipe_path)


####################
# Helper functions #
####################


def _check_recipe(script_path: Path, expected_recipe_path: Path) -> None:
    """Compares built recipe to the expected."""
    with open(expected_recipe_path) as expected_file:
        expected = expected_file.read()
    with open(script_path.with_suffix(".def")) as recipe_file:
        recipe = recipe_file.read()
    expected_lines = expected.split("\n")
    recipe_lines = recipe.split("\n")
    assert len(recipe_lines) == len(expected_lines)
    for i, expected_line in enumerate(expected_lines):
        assert recipe_lines[i].strip() == expected_line.strip()
