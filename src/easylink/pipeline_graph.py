import itertools
from pathlib import Path
from typing import Any, Dict, List, Tuple

import networkx as nx
from networkx import MultiDiGraph

from easylink.configuration import Config
from easylink.implementation import Implementation


class PipelineGraph(MultiDiGraph):
    def __init__(self, config: Config):
        super().__init__()
        self._create_graph(config)

    def _create_graph(self, config: Config) -> None:
        """Create a graph from the pipeline configuration."""
        schema = config.schema
        self.update(schema)
        # Update input data edges to direct to correct filenames from config
        for source, schema_node, data in self.out_edges("input_data_schema", data=True):
            for edge_idx in self[source][schema_node]:
                self[source][schema_node][edge_idx]["files"] = [
                    str(config.input_data[file]) for file in data["files"]
                ]

        for schema_node in schema.nodes:
            sub_graph = schema.get_attr(schema_node, "step").get_subgraph(config)
            # Connect the subgraph to the main graph
            self.update(sub_graph)
            sub_source_nodes = [
                node for node in sub_graph.nodes if sub_graph.in_degree(node) == 0
            ]
            input_edges = [
                (source, schema_node, data)
                for source, _, data in self.in_edges(schema_node, data=True)
            ]
            for source, _, data in input_edges:
                self.add_edges_from([(source, node, data) for node in sub_source_nodes])

            output_edges = [
                (schema_node, sink, data)
                for _, sink, data in self.out_edges(schema_node, data=True)
            ]
            sub_sink_nodes = [
                node for node in sub_graph.nodes if sub_graph.out_degree(node) == 0
            ]
            for _, sink, data in output_edges:
                for sub_node in sub_sink_nodes:
                    data["files"] = [
                        str(sub_graph.nodes[sub_node]["out_dir"] / file)
                        for file in data["files"]
                    ]
                    self.add_edge(sub_node, sink, data=data)

            # Remove the original node
            self.remove_node(schema_node)

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
                [data["data"]["files"] for _, _, data in self.in_edges(node, data=True)]
            )
        )
        output_files = list(
            itertools.chain.from_iterable(
                [data["data"]["files"] for _, _, data in self.out_edges(node, data=True)]
            )
        )
        return input_files, output_files
