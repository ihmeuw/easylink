from pathlib import Path
from typing import Callable

from easylink.configuration import Config
from easylink.implementation import Implementation
from easylink.pipeline_schema_constants import validate_input_file_dummy
from easylink.step import CompositeStep, ImplementedStep, InputStep, ResultStep


def test_input_step(default_config: Config) -> None:
    params = {
        "input_validator": validate_input_file_dummy,
        "out_dir": Path("input"),
    }
    step = InputStep("input", **params)
    assert step.name == "input"
    assert step.out_dir == Path("input")

    # Test get_subgraph
    subgraph = step.get_implementation_graph(default_config)
    assert list(subgraph.nodes) == ["input_data"]
    assert list(subgraph.edges) == []
    assert subgraph.nodes["input_data"]["input_validator"] == validate_input_file_dummy
    assert subgraph.nodes["input_data"]["out_dir"] == Path("input")


def test_result_step(default_config: Config) -> None:
    params = {
        "input_validator": validate_input_file_dummy,
        "out_dir": Path("results"),
    }
    step = ResultStep("results", **params)
    assert step.name == "results"
    assert step.out_dir == Path("results")

    # Test get_subgraph
    subgraph = step.get_implementation_graph(default_config)
    assert list(subgraph.nodes) == ["results"]
    assert list(subgraph.edges) == []
    assert subgraph.nodes["results"]["input_validator"] == validate_input_file_dummy
    assert subgraph.nodes["results"]["out_dir"] == Path("results")


def test_implemented_step(default_config: Config) -> None:
    params = {
        "input_validator": validate_input_file_dummy,
        "out_dir": Path("foo"),
    }
    step = ImplementedStep("step_1", **params)

    assert step.name == "step_1"
    assert step.out_dir == Path("foo")
    assert isinstance(step.input_validator, Callable)

    # Test get_subgraph
    subgraph = step.get_implementation_graph(default_config)
    imp_node = f"{step.name}_python_pandas"
    assert list(subgraph.nodes) == [imp_node]
    assert list(subgraph.edges) == []
    assert subgraph.nodes[imp_node]["input_validator"] == validate_input_file_dummy
    assert subgraph.nodes[imp_node]["out_dir"] == Path("foo") / imp_node
    implementation = subgraph.nodes[imp_node]["implementation"]
    assert isinstance(implementation, Implementation)
    assert implementation.name == imp_node
    assert implementation.schema_step_name == step.name


def test_composite_step(default_config: Config) -> None:
    params = {
        "step_1": {
            "step_type": ImplementedStep,
            "step_params": {
                "input_validator": validate_input_file_dummy,
                "out_dir": Path("intermediate"),
            },
        },
    }
    step = CompositeStep("composite", **params)
    assert step.name == "composite"
    assert step.out_dir == Path()

    # Test get_subgraph
    subgraph = step.get_implementation_graph(default_config)
    assert list(subgraph.nodes) == ["step_1_python_pandas"]
    assert list(subgraph.edges) == []
    assert (
        subgraph.nodes["step_1_python_pandas"]["input_validator"] == validate_input_file_dummy
    )
    assert (
        subgraph.nodes["step_1_python_pandas"]["out_dir"]
        == Path("intermediate") / "step_1_python_pandas"
    )
    implementation = subgraph.nodes["step_1_python_pandas"]["implementation"]
    assert isinstance(implementation, Implementation)
    assert implementation.name == "step_1_python_pandas"
    assert implementation.schema_step_name == "step_1"
