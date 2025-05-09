import os
import shutil
from pathlib import Path

import pytest
import yaml
from pytest_mock import MockerFixture

from easylink.devtools.implementation_creator import ImplementationCreator
from easylink.utilities.data_utils import load_yaml
from easylink.utilities.paths import IMPLEMENTATION_METADATA

GOOD_METADATA = """
# STEP_NAME: step_1
# REQUIREMENTS: pandas==2.1.2 pyarrow pyyaml"""

MISSING_METADATA = """
# step_name: step_1
# requirements: pandas==2.1.2 pyarrow pyyaml
// STEP_NAME: step_1
// REQUIREMENTS: pandas==2.1.2 pyarrow pyyaml
gosh I wish I'd capitalized the metadata keys or used the correct comment symbol!"""

MULTIPLE_METADATA = """
# STEP_NAME: step_1
# REQUIREMENTS: pandas==2.1.2 pyarrow pyyaml
# STEP_NAME: step_1
# REQUIREMENTS: pandas==2.1.2 pyarrow pyyaml"""

DIFFERENT_GOOD_METADATA = """
# STEP_NAME: step_1_different
# REQUIREMENTS: pandas==2.1.2 pyarrow pyyaml"""

MULTIPLE_STEPS_METADATA = """
# STEP_NAME: step_1, step_2
# REQUIREMENTS: pandas==2.1.2 pyarrow pyyaml"""


def test__extract_step_being_implemented(tmp_path: Path) -> None:
    script_path = tmp_path / "foo_step.py"
    with open(script_path, "w") as file:
        file.write(GOOD_METADATA)
    assert ImplementationCreator._extract_step_being_implemented(script_path) == "step_1"


@pytest.mark.parametrize(
    "script_content, error_msg",
    [
        (MISSING_METADATA, "Could not find a step name"),
        (MULTIPLE_METADATA, "Found multiple step_name requests"),
    ],
)
def test__extract_step_being_implemented_raises(
    script_content: str, error_msg: str, tmp_path: Path
) -> None:
    script_path = tmp_path / "foo_step.py"
    with open(script_path, "w") as file:
        file.write(script_content)
    with pytest.raises(ValueError, match=error_msg):
        ImplementationCreator._extract_step_being_implemented(script_path)


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
    creator = ImplementationCreator(script_path, Path("some-host"))
    creator.create_recipe()
    expected_recipe_path = (
        Path(os.path.dirname(__file__)) / "recipe_strings" / "python_pandas.txt"
    )
    _check_recipe(script_path, expected_recipe_path)


def test_register(tmp_path: Path, mocker: MockerFixture) -> None:

    script_path = tmp_path / "step_1_implementation.py"
    implementation_metadata = tmp_path / "test_implementation_metadata.yaml"
    # copy the real implementation metadata file to the test directory
    shutil.copy(IMPLEMENTATION_METADATA, implementation_metadata)

    def _write_test_metadata(info: dict[str, str | dict[str, str]]) -> None:
        # Don't accidentally write to the real implementation metadata file
        if implementation_metadata.resolve() == IMPLEMENTATION_METADATA.resolve():
            raise ValueError("Attempting to write to the real implementation metadata file")
        with open(str(implementation_metadata), "w") as f:
            yaml.dump(info, f, sort_keys=False)

    # write the script to be used for the test
    with open(script_path, "w") as file:
        file.write(GOOD_METADATA)

    creator = ImplementationCreator(script_path, Path("some-host"))

    assert "step_1_implementation" not in load_yaml(implementation_metadata)
    mocker.patch(
        "easylink.devtools.implementation_creator.ImplementationCreator._write_metadata",
        side_effect=_write_test_metadata,
    )
    creator.register()

    # load the new metadata and check it
    details = load_yaml(implementation_metadata)["step_1_implementation"]
    assert details == {
        "steps": ["step_1"],
        "image_path": "some-host/step_1_implementation.sif",
        "script_cmd": "python /step_1_implementation.py",
        "outputs": {"step_1_main_output": "result.parquet"},
    }

    # register a new version of the same implementation
    with open(script_path, "w") as file:
        file.write(DIFFERENT_GOOD_METADATA)

    creator2 = ImplementationCreator(script_path, Path("some-other-host"))

    # the implementation should be in there now
    assert "step_1_implementation" in load_yaml(implementation_metadata)
    creator2.register()
    details2 = load_yaml(implementation_metadata)["step_1_implementation"]
    assert details2 == {
        "steps": ["step_1_different"],
        "image_path": "some-other-host/step_1_implementation.sif",
        "script_cmd": "python /step_1_implementation.py",
        "outputs": {"step_1_different_main_output": "result.parquet"},
    }


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
