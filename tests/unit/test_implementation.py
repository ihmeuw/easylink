# mypy: ignore-errors
import pytest
from layered_config_tree import LayeredConfigTree
from pytest_mock import MockerFixture

from easylink.graph_components import OutputSlot
from easylink.implementation import Implementation


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
