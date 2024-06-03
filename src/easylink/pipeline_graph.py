import itertools
from pathlib import Path
from typing import Any, List, Tuple

import networkx as nx
from networkx import MultiDiGraph

from easylink.configuration import Config
from easylink.implementation import Implementation


class PipelineGraph(MultiDiGraph):
    def __init__(self, config):
        super().__init__()
        self._create_graph(config)

    def _create_graph(self, config: Config) -> None:
        """Create a graph from the pipeline configuration."""
        self.config = config
        self.add_node("input_data")
        prev_nodes = None

        for step in config.schema.steps:
            step_graph = step.get_subgraph(config)
            # Source nodes are nodes with no incoming edges ('in_degree' = 0)
            step_source_nodes = [node for node, deg in step_graph.in_degree() if deg == 0]
            # Add step graph to current graph without connections
            self.update(step_graph)
            for source_node in step_source_nodes:
                # Connect new source nodes to old sink nodes
                if step.prev_input:
                    for prev_node in prev_nodes:
                        self.add_edge(
                            prev_node,
                            source_node,
                            files=[str(Path("intermediate") / prev_node / "result.parquet")],
                        )
                # Add input data to the first step
                # This will probably need to be a node attribute in #TODO: [MIC-4774]
                if step.input_files:
                    self.add_edge(
                        "input_data",
                        source_node,
                        files=[str(file) for file in config.input_data],
                    )

            # Set prev_nodes to the sink nodes of the current step
            prev_nodes = [node for node, deg in self.out_degree() if deg == 0]
        # Add results node and connect to final sink nodes
        self.add_node("results")
        for node in prev_nodes:
            self.add_edge(node, "results", files=["result.parquet"])

    @property
    def implementation_nodes(self) -> List[str]:
        """Return list of nodes tied to specific implementations."""
        ordered_nodes = list(nx.topological_sort(self))
        return [node for node in ordered_nodes if node != "input_data" and node != "results"]

    @property
    def implementations(self) -> List[Implementation]:
        """Convenience property to get all implementations in the graph."""
        return [self.get_attr(node, "implementation") for node in self.implementation_nodes]

    def get_attr(self, node: str, attr: str) -> Any:
        """Convenience method to get a particular attribute from a node"""
        return self.nodes[node][attr]

    def get_input_output_files(self, node: str) -> Tuple[List[str], List[str]]:
        """Get all of a node's input and output files from edges."""
        input_files = list(
            itertools.chain.from_iterable(
                [data["files"] for _, _, data in self.in_edges(node, data=True)]
            )
        )
        output_files = list(
            itertools.chain.from_iterable(
                [data["files"] for _, _, data in self.out_edges(node, data=True)]
            )
        )
        return input_files, output_files
