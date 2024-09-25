from pathlib import Path
from typing import Dict, List, Optional

import networkx as nx
from layered_config_tree import LayeredConfigTree

from easylink.graph_components import ImplementationGraph
from easylink.pipeline_schema_constants import ALLOWED_SCHEMA_PARAMS
from easylink.step import CompositeStep, Step


class PipelineSchema(CompositeStep):
    """
    A schema is a nested graph that determines all possible
    allowable pipelines. The nodes of the graph are Steps, with
    edges representing file dependencies between then.
    """

    def __repr__(self) -> str:
        return f"PipelineSchema.{self.name}"

    def set_step_config(self, parent_config: LayeredConfigTree) -> None:
        self._config = parent_config

    def get_pipeline_graph(self, pipeline_config: LayeredConfigTree) -> ImplementationGraph:
        """Resolve the PipelineSchema into a PipelineGraph."""
        return self.get_implementation_graph()

    @property
    def step_nodes(self) -> List[str]:
        """Return list of nodes tied to specific implementations."""
        ordered_nodes = list(nx.topological_sort(self.step_graph))
        return [node for node in ordered_nodes if node != "input_data" and node != "results"]

    @property
    def steps(self) -> List[Step]:
        """Convenience property to get all steps in the graph."""
        return [self.step_graph.nodes[node]["step"] for node in self.step_nodes]

    @classmethod
    def _get_schemas(cls) -> List["PipelineSchema"]:
        """Creates the allowable schemas for the pipeline."""
        return [
            cls(name, nodes=nodes, edges=edges)
            for name, (nodes, edges) in ALLOWED_SCHEMA_PARAMS.items()
        ]

    def validate_inputs(self, input_data: Dict[str, Path]) -> Optional[List[str]]:
        "For each file slot used from the input data, validate the file's existence and properties."
        errors = {}
        for _, _, edge_attrs in self.step_graph.out_edges("input_data", data=True):
            validator = edge_attrs["input_slot"].validator
            for file in input_data.values():
                try:
                    validator(file)
                except FileNotFoundError as e:
                    errors[str(file)] = ["File not found."]
                except Exception as e:
                    errors[str(file)] = [e.args[0]]
        return errors


PIPELINE_SCHEMAS = PipelineSchema._get_schemas()
