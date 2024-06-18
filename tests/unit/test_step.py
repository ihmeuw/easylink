from pathlib import Path
from typing import Callable

import networkx as nx

from easylink.configuration import Config
from easylink.implementation import Implementation
from easylink.pipeline_schema_constants import validate_input_file_dummy
from easylink.step import CompositeStep, ImplementedStep, InputStep, ResultStep


def test_input_step(default_config: Config) -> None:
    params = {
        "input_slots": [],
        "output_slots": ["file1"],
    }
    step = InputStep("input", **params)
    assert step.name == "input"
    assert step.output_slots == ["file1"]
    assert step.input_slots == {}

    # Test get_subgraph
    subgraph = nx.MultiDiGraph()
    step.get_implementation_graph(subgraph, default_config)
    assert list(subgraph.nodes) == ["input_data"]
    assert list(subgraph.edges) == []


def test_result_step(default_config: Config) -> None:
    pass


def test_implemented_step(default_config: Config) -> None:
    pass


def test_composite_step(default_config: Config) -> None:
    pass
