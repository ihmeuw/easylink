from pathlib import Path
from typing import Any, List, Optional

import networkx as nx

from easylink.pipeline_schema_constants import ALLOWED_SCHEMA_PARAMS
from easylink.step import CompositeStep, Step


class PipelineSchema(CompositeStep):
    """Defines the allowable schema(s) for the pipeline."""

    def __repr__(self) -> str:
        return f"PipelineSchema.{self.name}"

    @property
    def step_nodes(self) -> List[str]:
        """Return list of nodes tied to specific implementations."""
        ordered_nodes = list(nx.topological_sort(self.graph))
        return [
            node
            for node in ordered_nodes
            if node != "input_data_schema" and node != "results_schema"
        ]

    @property
    def steps(self) -> List[Step]:
        """Convenience property to get all steps in the graph."""
        return [self.graph.nodes[node]["step"] for node in self.step_nodes]

    @classmethod
    def _get_schemas(cls) -> List["PipelineSchema"]:
        """Creates the allowable schema for the pipeline."""
        return [
            cls(name, **schema_params)
            for name, schema_params in ALLOWED_SCHEMA_PARAMS.items()
        ]

    def validate_input(self, filepath: Path) -> Optional[List[str]]:
        "Wrap the output file validator for now, since it is the same"
        try:
            self.graph.nodes["input_data_schema"]["step"].input_validator(filepath)
        except Exception as e:
            return [e.args[0]]


PIPELINE_SCHEMAS = PipelineSchema._get_schemas()
