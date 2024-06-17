from pathlib import Path
from typing import Any, Dict, List, Optional

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

    def get_sub_config(self, pipeline_config: LayeredConfigTree) -> LayeredConfigTree:
        """Return the subconfig for this step."""
        return pipeline_config

    def get_pipeline_graph(self, pipeline_config: LayeredConfigTree) -> nx.MultiDiGraph:
        """Resolve the PipelineSchema into a PipelineGraph."""
        graph = nx.MultiDiGraph()
        self.get_implementation_graph(graph, pipeline_config)
        return graph

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
        """Creates the allowable schemas for the pipeline."""
        return [
            cls(name, nodes=nodes, edges=edges)
            for name, (nodes, edges) in ALLOWED_SCHEMA_PARAMS.items()
        ]

    def validate_inputs(self, input_data: Dict[str, Path]) -> Optional[List[str]]:
        "Wrap the output file validator for now, since it is the same"
        errors = []
        for _, _, edge_data in self.graph.out_edges("input_data_schema", data=True):
            try:
                validator = edge_data["input_slot"].validator
                slot_name = edge_data["output_slot"]
                validator(input_data[slot_name])
            except Exception as e:
                errors.append(e.args[0])
        return errors


PIPELINE_SCHEMAS = PipelineSchema._get_schemas()
