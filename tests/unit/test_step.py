from typing import Callable

from easylink.implementation import Implementation
from easylink.step import ImplementedStep


def test_step_instantiation():
    params = {
        "input_validator": lambda *_: None,
        "out_dir": "results",
    }
    step = ImplementedStep("foo", **params)
    assert step.name == "foo"
    assert step.out_dir == "results"
    assert isinstance(step.input_validator, Callable)


def test_get_subgraph(default_config):
    schema = default_config.schema
    for step in schema.steps:
        subgraph = step.get_subgraph(default_config)
        assert len(subgraph.nodes) == 1
        assert len(subgraph.edges) == 0
        imp_node = f"{step.name}_python_pandas"
        implementation = subgraph.nodes[imp_node]["implementation"]
        assert isinstance(implementation, Implementation)
        assert implementation.name == imp_node
        assert implementation.schema_step_name == step.name
