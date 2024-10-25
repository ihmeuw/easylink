import itertools
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple, Union

import networkx as nx

from easylink.configuration import Config
from easylink.graph_components import EdgeParams, ImplementationGraph, InputSlot
from easylink.implementation import Implementation


class PipelineGraph(ImplementationGraph):
    """
    The Pipeline Graph is the structure of the pipeline. It is a DAG composed of
    Implementations and their file dependencies. The Pipeline Graph is created by
    "flattening" the Pipeline Schema (a nested Step Graph) with parameters set in
    the configuration.

    """

    def __init__(self, config: Config) -> None:
        super().__init__(incoming_graph_data=config.schema.get_implementation_graph())
        self.merge_joint_implementations(config)
        self.update_slot_filepaths(config)
        self = nx.freeze(self)

    def merge_joint_implementations(self, config):
        for (
            combined_implementation,
            joint_implementation_config,
        ) in config.pipeline.combined_implementations.items():

            # Find all nodes with the same implementation name
            nodes_to_merge = [
                node
                for node, data in self.nodes(data=True)
                if data["implementation"].combined_name == combined_implementation
            ]

            implemented_steps = [
                step
                for node in nodes_to_merge
                for step in self.nodes[node]["implementation"].schema_steps
            ]
            input_slots = []
            output_slots = []
            in_edge_params = []
            combined_edges = []

            for node in nodes_to_merge:
                for pred, succ, data in self.in_edges(node, data=True):
                    if pred not in nodes_to_merge:
                        input_slots.append(data["input_slot"])
                        in_edge_params.append(EdgeParams.from_graph_edge(pred, succ, data))
                        combined_edges.append(EdgeParams.from_graph_edge(pred, succ, data))

                for _, succ, data in self.out_edges(node, data=True):
                    if succ not in nodes_to_merge:
                        output_slots.append(data["output_slot"])
                        combined_edges.append(
                            EdgeParams.from_graph_edge(combined_implementation, succ, data)
                        )
            self.validate_edge_params(in_edge_params)

            for slots in (input_slots, output_slots):
                seen_slots = []
                seen_names = []
                seen_env_vars = []
                for slot in slots:
                    # Check for duplicate names
                    if slot.name in seen_names and slot not in seen_slots:
                        raise ValueError(f"Duplicate slot name found: '{slot.name}'")
                    seen_names.append(slot.name)

                    # Check for duplicate env_vars
                    if isinstance(slot, InputSlot):
                        if slot.env_var in seen_env_vars and slot not in seen_slots:
                            raise ValueError(
                                f"Duplicate environment variable found: '{slot.env_var}'"
                            )
                        seen_env_vars.append(slot.env_var)
                    seen_slots.append(slot)

            new_implementation = Implementation(
                implemented_steps, joint_implementation_config, input_slots, output_slots
            )
            self.add_node(combined_implementation, implementation=new_implementation)

            # Redirect edges
            for edge in combined_edges:
                self.add_edge_from_params(edge)

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

    def validate_edge_params(self, edge_params: list[EdgeParams]) -> None:
        # Group edges by output slot to check for conflicts
        input_slot_groups = defaultdict(list)

        for edge in edge_params:
            input_slot_groups[edge.input_slot].append(edge)

        # Check each group of edges sharing the same output slot
        for input_slot, edges in input_slot_groups.items():
            if len(edges) <= 1:
                continue

            # Check for conflicting input slots
            output_slot_names = set(edge.output_slot for edge in edges)
            if len(output_slot_names) > 1:
                conflicting_edges = [
                    f"({edge.source_node}->{edge.target_node}, output={edge.output_slot})"
                    for edge in edges
                ]
                raise ValueError(
                    f"Input slot '{input_slot}' is connected to different output slots in edges: {', '.join(conflicting_edges)}"
                )

            edge_schema_steps = set(
                tuple(self.nodes[edge.source_node]["implementation"].schema_steps)
                for edge in edges
            )
            if len(edge_schema_steps) > 1:
                raise ValueError(
                    f"Input slot '{input_slot}' is connected to nodes that implement different schema steps"
                )
