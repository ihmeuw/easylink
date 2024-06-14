import itertools
from typing import Any, Dict, List, Tuple

import networkx as nx
from networkx import MultiDiGraph

from easylink.configuration import Config
from easylink.implementation import Implementation


class PipelineGraph(MultiDiGraph):
    """
    The Pipeline Graph is the structure of the pipeline. It is a DAG composed of
    Implementations and their file dependencies. The Pipeline Graph is created by
    "flattening" the Pipeline Schema (a nested Step Graph) with parameters set in
    the configuration.

    """

    def __init__(self, config: Config) -> None:
        super().__init__(incoming_graph_data=config.schema.get_subgraph(config))
        # Update input data edges to direct to correct filenames from config
        for source, schema_node, data in self.out_edges("input_data", data=True):
            for edge_idx in self[source][schema_node]:
                self[source][schema_node][edge_idx]["files"] = [
                    str(config.input_data[file]) for file in data["files"]
                ]

    @property
    def implementation_nodes(self) -> List[str]:
        """Return list of nodes tied to specific implementations."""
        ordered_nodes = list(nx.topological_sort(self))
        return [node for node in ordered_nodes if node != "input_data" and node != "results"]

    @property
    def implementations(self) -> List[Implementation]:
        """Convenience property to get all implementations in the graph."""
        return [self.nodes[node]["implementation"] for node in self.implementation_nodes]

    def get_input_slots(self, node: str) -> Dict[str, List[str]]:
        """Get all of a node's input slots from edges."""
        input_slots = {}
        for _, _, data in self.in_edges(node, data=True):
            # Consider whether we need duplicate variables to merge
            env_var, files = data["env_var"], data["files"]
            if env_var in input_slots:
                input_slots[env_var].extend(files)
            else:
                input_slots[env_var] = files
        return input_slots

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

    def spark_is_required(self) -> bool:
        """Check if the pipeline requires spark resources."""
        return any([implementation.requires_spark for implementation in self.implementations])
