from pathlib import Path
from typing import Dict, List, Optional

import networkx as nx
from layered_config_tree import LayeredConfigTree

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

    def get_pipeline_graph(self, pipeline_config: LayeredConfigTree) -> nx.MultiDiGraph:
        """Resolve the PipelineSchema into a PipelineGraph."""
        graph = nx.MultiDiGraph()
        graph.add_node(self.name, step=self)
        self.update_implementation_graph(graph, pipeline_config)
        return graph

    @property
    def step_nodes(self) -> List[str]:
        """Return list of nodes tied to specific implementations."""
        ordered_nodes = list(nx.topological_sort(self.graph))
        return [node for node in ordered_nodes if node != "input_data" and node != "results"]

    @property
    def steps(self) -> List[Step]:
        """Convenience property to get all steps in the graph."""
        return [self.graph.nodes[node]["step"] for node in self.step_nodes]

    @classmethod
    def _get_schemas(cls) -> List["PipelineSchema"]:
        """Creates the allowable schemas for the pipeline."""
        return [
            cls(name, nodes=nodes, edges=edges)
            for name, (nodes, edges) in ALLOWED_SCHEMA_PARAMS.items()
        ]

    def validate_inputs(self, input_data: Dict[str, Path]) -> Optional[List[str]]:
        "For each file slot used from the input data, validate that the file's existence and properties."
        errors = {}
        for _, _, edge_attrs in self.graph.out_edges("input_data", data=True):
            validator = edge_attrs["input_slot"].validator
            slot_name = edge_attrs["output_slot"].name
            try:
                file = input_data[slot_name]
            except KeyError:
                errors[str(slot_name)] = ["Missing required input data"]
                continue
            if not file.exists():
                errors[str(file)] = ["File not found."]
                continue
            try:
                validator(file)
            except Exception as e:
                errors[str(file)] = [e.args[0]]
        return errors


PIPELINE_SCHEMAS = PipelineSchema._get_schemas()
