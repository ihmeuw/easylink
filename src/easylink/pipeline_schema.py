from pathlib import Path
from typing import List, Optional, Any
import networkx as nx
from networkx import MultiDiGraph
from easylink.step import Step
from easylink.pipeline_schema_constants import ALLOWED_SCHEMA_PARAMS


class PipelineSchema(MultiDiGraph):
    """Defines the allowable schema(s) for the pipeline."""

    def __init__(self, name: str, schema_params: dict) -> None:
        super().__init__(name=name, input_validator=schema_params["input_validator"])
        self.add_node("input_data")
        for step_name, step_params in schema_params["steps"].items():
            self.add_node(step_name, step=Step(step_name, input_validator=step_params["input_validator"]))
            for in_edge, edge_params in step_params["in_edges"].items():
                self.add_edge(in_edge, step_name, **edge_params)

    def __repr__(self) -> str:
        return f"PipelineSchema.{self.name}"

    @property
    def step_nodes(self) -> List[str]:
        """Return list of nodes tied to specific implementations."""
        ordered_nodes = list(nx.topological_sort(self))
        return [node for node in ordered_nodes if node != "input_data" and node != "results"]

    @property
    def steps(self) -> List[Step]:
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

    def get_step_id(self, step: Step) -> str:
        idx = self.steps.index(step)
        step_number = str(idx + 1).zfill(len(str(len(self))))
        return f"{step_number}_{step.name}"

    def validate_input(self, filepath: Path) -> Optional[List[str]]:
        "Wrap the output file validator for now, since it is the same"
        input_validator = self.nodes["input_data"]["input_validator"]
        try:
            input_validator(filepath)
        except Exception as e:
            return [e.args[0]]


PIPELINE_SCHEMAS = PipelineSchema._get_schemas()
