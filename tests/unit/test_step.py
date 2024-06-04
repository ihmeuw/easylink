from typing import Callable

from easylink.implementation import Implementation
from easylink.step import Step


def test_step_instantiation():
    step = Step("foo", prev_input=True, input_files=True)
    assert step.name == "foo"
    assert step.prev_input
    assert step.input_files
    assert isinstance(step.input_validator, Callable)


def test_get_subgraph(default_config):
    schema = default_config.schema
    for step in schema.steps:
        subgraph = step.get_subgraph(default_config)
        assert len(subgraph.nodes) == 1
        assert len(subgraph.edges) == 0
        implementation = subgraph.nodes[schema.get_step_id(step)]["implementation"]
        assert isinstance(implementation, Implementation)
        assert implementation.name == f"{step.name}_python_pandas"
        assert implementation.schema_step_name == step.name
