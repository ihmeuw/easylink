import os
import shutil
from pathlib import Path

import pytest
import yaml
from pytest_mock import MockerFixture

from easylink.devtools.implementation_creator import ImplementationCreator
from easylink.utilities.data_utils import load_yaml
from easylink.utilities.paths import DEV_IMAGES_DIR, IMPLEMENTATION_METADATA

GOOD_METADATA = """
# STEP_NAME: step_1
# REQUIREMENTS: pandas==2.1.2 pyarrow pyyaml
# PIPELINE_SCHEMA: development"""

MISSING_METADATA = """
# step_name: step_1
# requirements: pandas==2.1.2 pyarrow pyyaml
# pipeline_schema: testing
// STEP_NAME: step_1
// REQUIREMENTS: pandas==2.1.2 pyarrow pyyaml
// PIPELINE_SCHEMA: testing
gosh I wish I'd capitalized the metadata keys or used the correct comment symbol!"""

MULTIPLE_METADATA = """
# STEP_NAME: step_1
# REQUIREMENTS: pandas==2.1.2 pyarrow pyyaml
# STEP_NAME: step_1
# REQUIREMENTS: pandas==2.1.2 pyarrow pyyaml"""

MULTIPLE_STEPS_METADATA = """
# STEP_NAME: step_1, step_2
# REQUIREMENTS: pandas==2.1.2 pyarrow pyyaml"""


def test__extract_implemented_step(tmp_path: Path) -> None:
    script_path = tmp_path / "foo_step.py"
    with open(script_path, "w") as file:
        file.write(GOOD_METADATA)
    assert ImplementationCreator._extract_implemented_step(script_path) == "step_1"


@pytest.mark.parametrize(
    "script_content, error_msg",
    [
        (MISSING_METADATA, "Could not find a step name"),
        (MULTIPLE_METADATA, "Found multiple step_name requests"),
    ],
)
def test__extract_implemented_step_raises(
    script_content: str, error_msg: str, tmp_path: Path
) -> None:
    script_path = tmp_path / "foo_step.py"
    with open(script_path, "w") as file:
        file.write(script_content)
    with pytest.raises(ValueError, match=error_msg):
        ImplementationCreator._extract_implemented_step(script_path)


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


@pytest.mark.parametrize(
    "script_content, expected",
    [
        (GOOD_METADATA, "development"),
        (MISSING_METADATA, "main"),
    ],
)
def test__extract_pipeline_schema_name(
    script_content: str, expected: str, tmp_path: Path
) -> None:
    script_path = tmp_path / "foo_step.py"
    with open(script_path, "w") as file:
        file.write(script_content)
    assert ImplementationCreator._extract_pipeline_schema_name(script_path) == expected


def test__extract_pipeline_schema_name_raises(tmp_path: Path) -> None:
    script_path = tmp_path / "foo_step.py"
    metadata_str = GOOD_METADATA.replace("development", "some-non-existing-schema")
    with open(script_path, "w") as file:
        file.write(metadata_str)
    with pytest.raises(ValueError, match="is not supported"):
        ImplementationCreator._extract_pipeline_schema_name(script_path)


def test__extract_implementable_steps(tmp_path: Path) -> None:
    script_path = tmp_path / "foo_step.py"
    with open(script_path, "w") as file:
        file.write(GOOD_METADATA)
    steps = ImplementationCreator._extract_implementable_steps("development")

    assert [step.name for step in steps] == [
        "step_1",
        "step_2",
        "step_3",
        "step_4",
        "step_4a",
        "step_4b",
        "step_5_and_6",
        "step_5",
        "step_6",
    ]


@pytest.mark.parametrize(
    "step_name",
    [
        "step_1",
        "step_2",
        "step_3",
        "step_4",
        "step_4a",
        "step_4b",
        "step_5_and_6",
        "step_5",
        "step_6",
    ],
)
def test__extract_output_slot(step_name: str, tmp_path: Path) -> None:
    script_path = tmp_path / "foo_step.py"
    # Replace the hard-coded "step_1" with the parameterized step_name
    metadata = GOOD_METADATA.replace("step_1", step_name)
    with open(script_path, "w") as file:
        file.write(metadata)
    assert (
        ImplementationCreator._extract_output_slot(script_path, step_name)
        == f"{step_name}_main_output"
    )


def test__extract_output_slot_raises(tmp_path: Path) -> None:
    script_path = tmp_path / "foo_step.py"
    with open(script_path, "w") as file:
        file.write(GOOD_METADATA)
    with pytest.raises(
        ValueError,
        match="does not exist as an implementable step",
    ):
        ImplementationCreator._extract_output_slot(script_path, "foo_step")


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

    script_path = tmp_path / "test_implementation.py"
    implementation_metadata = tmp_path / "test_implementation_metadata.yaml"
    # copy the real implementation metadata file to the test directory
    shutil.copy(IMPLEMENTATION_METADATA, implementation_metadata)

    # Don't accidentally write to the real implementation metadata file
    def _write_test_metadata(info: dict[str, str | dict[str, str]]) -> None:
        if implementation_metadata.resolve() == IMPLEMENTATION_METADATA.resolve():
            raise ValueError("Attempting to write to the real implementation metadata file")
        with open(str(implementation_metadata), "w") as f:
            yaml.dump(info, f, sort_keys=False)

    mocker.patch(
        "easylink.devtools.implementation_creator.ImplementationCreator._write_metadata",
        side_effect=_write_test_metadata,
    )
    mocker.patch(
        "easylink.devtools.implementation_creator.load_yaml",
        return_value=load_yaml(implementation_metadata),
    )

    # write the script to be used for the test
    with open(script_path, "w") as file:
        file.write(GOOD_METADATA)

    creator = ImplementationCreator(script_path, Path("some-host"))
    assert "test_implementation" not in load_yaml(implementation_metadata)
    creator.register()

    # load the new metadata and check it
    details = load_yaml(implementation_metadata)["test_implementation"]
    assert details == {
        "steps": ["step_1"],
        "image_name": "test_implementation.sif",
        "script_cmd": "python /test_implementation.py",
        "outputs": {"step_1_main_output": "result.parquet"},
    }

    # register a new version of the same implementation
    new_script_path = tmp_path / "test_new_implementation.py"
    with open(new_script_path, "w") as file:
        file.write(GOOD_METADATA)

    new_creator = ImplementationCreator(new_script_path, Path("some-other-host"))

    # the original implementation should not be overwritten
    md = load_yaml(implementation_metadata)
    assert "test_implementation" in md
    assert "test_new_implementation" not in md
    new_creator.register()

    md = load_yaml(implementation_metadata)
    assert "test_implementation" in md
    assert md["test_new_implementation"] == {
        "steps": ["step_1"],
        "image_name": "test_new_implementation.sif",
        "script_cmd": "python /test_new_implementation.py",
        "outputs": {"step_1_main_output": "result.parquet"},
    }


def test_register_to_dev_folder_subdir(tmp_path: Path, mocker: MockerFixture) -> None:

    implementation_metadata = tmp_path / "test_implementation_metadata.yaml"
    # copy the real implementation metadata file to the test directory
    shutil.copy(IMPLEMENTATION_METADATA, implementation_metadata)

    # Don't accidentally write to the real implementation metadata file
    def _write_test_metadata(info: dict[str, str | dict[str, str]]) -> None:
        if implementation_metadata.resolve() == IMPLEMENTATION_METADATA.resolve():
            raise ValueError("Attempting to write to the real implementation metadata file")
        with open(str(implementation_metadata), "w") as f:
            yaml.dump(info, f, sort_keys=False)

    mocker.patch(
        "easylink.devtools.implementation_creator.ImplementationCreator._write_metadata",
        side_effect=_write_test_metadata,
    )
    mocker.patch(
        "easylink.devtools.implementation_creator.load_yaml",
        return_value=load_yaml(implementation_metadata),
    )

    dev_script = tmp_path / "some_fancy_new_implementation.py"
    with open(dev_script, "w") as file:
        file.write(GOOD_METADATA)

    dev_creator = ImplementationCreator(dev_script, Path(DEV_IMAGES_DIR) / "some-subdir")

    md = load_yaml(implementation_metadata)
    assert "some_fancy_new_implementation" not in md
    dev_creator.register()

    md = load_yaml(implementation_metadata)
    assert md["some_fancy_new_implementation"] == {
        "steps": ["step_1"],
        "image_name": "some-subdir/some_fancy_new_implementation.sif",
        "script_cmd": "python /some_fancy_new_implementation.py",
        "outputs": {"step_1_main_output": "result.parquet"},
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
