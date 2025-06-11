# mypy: ignore-errors
import pytest
from layered_config_tree import LayeredConfigTree
from pytest_mock import MockerFixture

from easylink.graph_components import OutputSlot
from easylink.implementation import Implementation
from easylink.utilities.data_utils import load_yaml
from easylink.utilities.paths import DEFAULT_IMAGES_DIR, IMPLEMENTATION_METADATA


def test__get_env_vars(mocker):
    mocker.patch(
        "easylink.implementation.load_yaml",
        return_value={
            "test": {"steps": ["this_step"], "env": {"foo": "corge", "spam": "eggs"}}
        },
    )
    implementation = Implementation(
        schema_steps=["this_step"],
        implementation_config=LayeredConfigTree(
            {"name": "test", "configuration": {"foo": "bar", "baz": "qux"}}
        ),
        input_slots=[],
        output_slots=[],
    )
    assert implementation.environment_variables == {
        "foo": "bar",
        "baz": "qux",
        "spam": "eggs",
    }


@pytest.mark.parametrize(
    "output_slots, metadata_outputs, expected_outputs",
    [
        # one slot, defined output
        (
            ["main_output"],
            {"main_output": "main/output/filepath"},
            {"main_output": "main/output/filepath"},
        ),
        # one slot, undefined output
        (["main_output"], None, {"main_output": "."}),
        # two slots, defined outputs
        (
            ["main_output", "secondary_output"],
            {
                "main_output": "main/output/filepath",
                "secondary_output": "secondary/output/filepath",
            },
            {
                "main_output": "main/output/filepath",
                "secondary_output": "secondary/output/filepath",
            },
        ),
        # two slots, one defined and one undefined output
        (
            ["main_output", "secondary_output"],
            {"secondary_output": "secondary/output/filepath"},
            {"main_output": "main_output", "secondary_output": "secondary/output/filepath"},
        ),
        # two slots, both undefined outputs
        (
            ["main_output", "secondary_output"],
            None,
            {"main_output": "main_output", "secondary_output": "secondary_output"},
        ),
    ],
)
def test_outputs(
    output_slots: list[str],
    metadata_outputs: dict[str, str],
    expected_outputs: dict[str, str],
    mocker: MockerFixture,
):
    """Test that the outputs property returns the correct output details."""
    mocker.patch(
        "easylink.implementation.Implementation._load_metadata",
        return_value={
            "steps": [
                "this_step",
            ],
            "image_path": "/path/to/image.sif",
            "script_cmd": "python /path/to/script.py",
            **({"outputs": metadata_outputs} if metadata_outputs else {}),
        },
    )
    implementation = Implementation(
        schema_steps=["this_step"],
        implementation_config=LayeredConfigTree({"name": "test"}),
        output_slots=[OutputSlot(slot) for slot in output_slots],
    )
    assert implementation.outputs == expected_outputs


def test__handle_conflicting_checksums(mocker: MockerFixture, caplog):
    """Test that this will re-download an image appropriately."""
    # Mock so we don't accidentally download an image
    download_image_mock = mocker.patch("easylink.implementation.download_image")
    assert download_image_mock.call_count == 0
    # Mock the checksum calculation to return a different value than expected
    mocker.patch(
        "easylink.implementation.calculate_md5_checksum", return_value="ducks-go-quack"
    )
    expected_md5_checksum = "cows-go-moo"
    logs = []
    metadata = load_yaml(IMPLEMENTATION_METADATA)
    Implementation._handle_conflicting_checksums(
        logs=logs,
        image_path=(DEFAULT_IMAGES_DIR / "python_pandas.sif"),
        expected_md5_checksum=expected_md5_checksum,
        record_id=metadata["step_1_python_pandas"]["zenodo_record_id"],
    )
    assert (
        "/.easylink_images/python_pandas.sif' exists but has a different MD5 checksum (ducks-go-quack) than expected (cows-go-moo). Re-downloading the image."
        in caplog.text
    )
    assert download_image_mock.call_count == 1
