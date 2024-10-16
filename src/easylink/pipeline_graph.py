import itertools
from pathlib import Path
from typing import Dict, List, Tuple, Union

import networkx as nx

from easylink.configuration import Config
from easylink.graph_components import ImplementationGraph, InputSlot
from easylink.implementation import Implementation
from easylink.utilities import paths
from easylink.utilities.data_utils import load_yaml


class PipelineGraph(ImplementationGraph):
    """
    The Pipeline Graph is the structure of the pipeline. It is a DAG composed of
    Implementations and their file dependencies. The Pipeline Graph is created by
    "flattening" the Pipeline Schema (a nested Step Graph) with parameters set in
    the configuration.

    """

    def __init__(self, config: Config) -> None:
        super().__init__(incoming_graph_data=config.schema.get_implementation_graph())
        self.merge_joint_implementations()
        self.update_slot_filepaths(config)
        self = nx.freeze(self)

    def merge_joint_implementations(self):
        implementation_metadata = load_yaml(paths.IMPLEMENTATION_METADATA)
        # Find all joint implementations
        joint_implementations = {
            data["implementation"].name
            for node, data in self.nodes(data=True)
            if data["implementation"].is_joint
        }

        for joint_implementation in joint_implementations:

            # Find all nodes with the same implementation name
            nodes_to_merge = [
                node
                for node, data in self.nodes(data=True)
                if data["implementation"].name == joint_implementation
            ]

            # Check if metadata_steps match
            implementation_metadata_steps = set(
                implementation_metadata[joint_implementation]["steps"]
            )
            node_set = set(nodes_to_merge)
            implemented_steps = set(
                step
                for node in nodes_to_merge
                for step in self.nodes[node]["implementation"].schema_steps
            )
            if implementation_metadata_steps != implemented_steps:
                raise ValueError(
                    f"Metadata steps don't match for implementation {joint_implementation}"
                )

            # Create a new node
            new_node = f"merged_{joint_implementation}"
            new_implementation = Implementation.merge_implementations(
                [self.nodes[node]["implementation"] for node in nodes_to_merge]
            )
            self.add_node(new_node, implementation=new_implementation)

            # Redirect edges
            for node in nodes_to_merge:
                for pred, _, data in self.in_edges(node, data=True):
                    if pred not in nodes_to_merge:
                        self.add_edge(pred, new_node, **data)

                for _, succ, data in self.out_edges(node, data=True):
                    if succ not in nodes_to_merge:
                        self.add_edge(new_node, succ, **data)

            # Remove original nodes
            self.remove_nodes_from(nodes_to_merge)
            try:
                cycle = nx.find_cycle(self)
                if cycle:
                    raise ValueError("The MultiDiGraph contains a cycle: {}".format(cycle))
            except nx.NetworkXNoCycle:
                pass

    def update_slot_filepaths(self, config: Config) -> None:
        """Fill graph edges with appropriate filepath information."""
        # Update input data edges to direct to correct filenames from config
        for source, sink, edge_attrs in self.out_edges("input_data", data=True):
            for edge_idx in self[source][sink]:
                if edge_attrs["output_slot"].name == "all":
                    self[source][sink][edge_idx]["filepaths"] = tuple(
                        str(path) for path in config.input_data.to_dict().values()
                    )
                else:
                    self[source][sink][edge_idx]["filepaths"] = (
                        str(config.input_data[edge_attrs["output_slot"].name]),
                    )

        # Update implementation nodes with yaml metadata
        for node in self.implementation_nodes:
            imp_outputs = self.nodes[node]["implementation"].outputs
            for source, sink, edge_attrs in self.out_edges(node, data=True):
                for edge_idx in self[node][sink]:
                    self[source][sink][edge_idx]["filepaths"] = (
                        str(
                            Path("intermediate")
                            / node
                            / imp_outputs[edge_attrs["output_slot"].name]
                        ),
                    )

    def get_input_slots(self, node: str) -> dict[str, dict[str, Union[str, list[str]]]]:
        """Get all of a node's input slots from edges."""
        input_slots = [
            edge_attrs["input_slot"] for _, _, edge_attrs in self.in_edges(node, data=True)
        ]
        filepaths_by_slot = [
            list(edge_attrs["filepaths"])
            for _, _, edge_attrs in self.in_edges(node, data=True)
        ]
        return self.condense_input_slots(input_slots, filepaths_by_slot)

    @staticmethod
    def condense_input_slots(
        input_slots: List[InputSlot], filepaths_by_slot: List[str]
    ) -> Dict[str, dict[str, Union[str, list[str]]]]:
        condensed_slot_dict = {}
        for input_slot, filepaths in zip(input_slots, filepaths_by_slot):
            slot_name, env_var, validator = (
                input_slot.name,
                input_slot.env_var,
                input_slot.validator,
            )
            if slot_name in condensed_slot_dict:
                if env_var != condensed_slot_dict[slot_name]["env_var"]:
                    raise ValueError(
                        f"Duplicate slot name {slot_name} with different env vars."
                    )
                if validator != condensed_slot_dict[slot_name]["validator"]:
                    raise ValueError(
                        f"Duplicate slot name {slot_name} with different validators."
                    )
                condensed_slot_dict[slot_name]["filepaths"].extend(filepaths)
            else:
                condensed_slot_dict[slot_name] = {
                    "env_var": env_var,
                    "validator": validator,
                    "filepaths": filepaths,
                }
        return condensed_slot_dict

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
