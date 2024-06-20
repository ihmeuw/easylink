import itertools
from pathlib import Path
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
        super().__init__(
            incoming_graph_data=config.schema.get_pipeline_graph(config.pipeline)
        )
        self.update_slot_filepaths(config)

    @property
    def implementation_nodes(self) -> List[str]:
        """Return list of nodes tied to specific implementations."""
        ordered_nodes = list(nx.topological_sort(self))
        return [node for node in ordered_nodes if node != "input_data" and node != "results"]

    @property
    def implementations(self) -> List[Implementation]:
        """Convenience property to get all implementations in the graph."""
        return [self.nodes[node]["implementation"] for node in self.implementation_nodes]

    def update_slot_filepaths(self, config: Config) -> None:
        # Update input data edges to direct to correct filenames from config
        for source, sink, edge_attrs in self.out_edges("input_data", data=True):
            for edge_idx in self[source][sink]:
                self[source][sink][edge_idx]["filepaths"] = [
                    str(config.input_data[edge_attrs["output_slot"]])
                ]

        # Update implementation nodes with yaml metadata
        for node in self.implementation_nodes:
            imp_outputs = self.nodes[node]["implementation"].outputs
            for source, sink, edge_attrs in self.out_edges(node, data=True):
                for edge_idx in self[node][sink]:
                    self[source][sink][edge_idx]["filepaths"] = [
                        str(
                            Path("intermediate")
                            / node
                            / imp_outputs[edge_attrs["output_slot"]]
                        )
                    ]

    def get_input_slots(self, node: str) -> Dict[str, List[str]]:
        """Get all of a node's input slots from edges."""
        input_slots = {}
        for _, _, edge_attrs in self.in_edges(node, data=True):
            # Consider whether we need duplicate variables to merge
            env_var, files = edge_attrs["input_slot"].env_var, edge_attrs["filepaths"]
            if env_var in input_slots:
                input_slots[env_var].extend(files)
            else:
                input_slots[env_var] = files
        return input_slots

    def get_input_output_files(self, node: str) -> Tuple[List[str], List[str]]:
        """Get all of a node's input and output files from edges."""
        input_files = list(
            itertools.chain.from_iterable(
                [
                    edge_attrs["filepaths"]
                    for _, _, edge_attrs in self.in_edges(node, data=True)
                ]
            )
        )
        output_files = list(
            itertools.chain.from_iterable(
                [
                    edge_attrs["filepaths"]
                    for _, _, edge_attrs in self.out_edges(node, data=True)
                ]
            )
        )
        return input_files, output_files

    def spark_is_required(self) -> bool:
        """Check if the pipeline requires spark resources."""
        return any([implementation.requires_spark for implementation in self.implementations])
