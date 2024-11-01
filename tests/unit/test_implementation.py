from layered_config_tree import LayeredConfigTree

from easylink.implementation import Implementation


def test__get_env_vars(mocker):
    mocker.patch(
        "easylink.implementation.load_yaml",
        return_value={"test": {"step": "this_step", "env": {"foo": "corge", "spam": "eggs"}}},
    )
    implementation = Implementation(
        step_name="this_step",
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
