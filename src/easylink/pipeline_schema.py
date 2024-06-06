from pathlib import Path
from typing import Any, List, Optional

import networkx as nx
from networkx import MultiDiGraph

from easylink.pipeline_schema_constants import ALLOWED_SCHEMA_PARAMS
from easylink.step import AbstractStep


class PipelineSchema(MultiDiGraph):
    """Defines the allowable schema(s) for the pipeline."""

    def __init__(self, name: str, schema_params: dict) -> None:
        super().__init__(name=name)
        for step_name, step_params in schema_params.items():
            self.add_node(
                step_name,
                step=step_params["step_type"](
                    step_name,
                    input_validator=step_params["input_validator"],
                    out_dir=step_params["out_dir"],
                ),
            )
            for in_edge, edge_params in step_params["in_edges"].items():
                self.add_edge(in_edge, step_name, **edge_params)

    def __repr__(self) -> str:
        return f"PipelineSchema.{self.name}"

    @property
    def step_nodes(self) -> List[str]:
        """Return list of nodes tied to specific implementations."""
        ordered_nodes = list(nx.topological_sort(self))
        return [
            node
            for node in ordered_nodes
            if node != "input_data_schema" and node != "results_schema"
        ]

    @property
    def steps(self) -> List[AbstractStep]:
        """Convenience property to get all steps in the graph."""
        return [self.get_attr(node, "step") for node in self.step_nodes]

    def get_attr(self, node: str, attr: str) -> Any:
        """Convenience method to get a particular attribute from a node"""
        return self.nodes[node][attr]

    @classmethod
    def _get_schemas(cls) -> List["PipelineSchema"]:
        """Creates the allowable schema for the pipeline."""
        return [
            cls(name, schema_params) for name, schema_params in ALLOWED_SCHEMA_PARAMS.items()
        ]

    def validate_input(self, filepath: Path) -> Optional[List[str]]:
        "Wrap the output file validator for now, since it is the same"
        try:
            self.nodes["input_data_schema"]["step"].input_validator(filepath)
        except Exception as e:
            return [e.args[0]]


PIPELINE_SCHEMAS = PipelineSchema._get_schemas()
