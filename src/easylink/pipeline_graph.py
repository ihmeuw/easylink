import itertools
from collections import Counter, defaultdict
from copy import deepcopy
from pathlib import Path
from typing import Dict, Iterable, List, Tuple, Union

import networkx as nx

from easylink.configuration import Config
from easylink.graph_components import (
    EdgeParams,
    ImplementationGraph,
    InputSlot,
    InputSlotMapping,
    OutputSlot,
    OutputSlotMapping,
)
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
        self.merge_combined_implementations(config)
        self.update_slot_filepaths(config)
        self = nx.freeze(self)

    def merge_combined_implementations(self, config):
        for (
            combined_implementation,
            combined_implementation_config,
        ) in config.pipeline.combined_implementations.items():

            # Find all nodes with the same combined implementation
            nodes_to_merge = [
                node
                for node, data in self.nodes(data=True)
                if data["implementation"].combined_name == combined_implementation
            ]
            if not nodes_to_merge:
                continue

            implemented_steps = [
                step
                for node in nodes_to_merge
                for step in self.nodes[node]["implementation"].schema_steps
            ]
            metadata_steps = self.nodes[nodes_to_merge[0]]["implementation"].metadata_steps
            self.validate_implementation_topology(nodes_to_merge, metadata_steps)

            (
                combined_input_slots,
                combined_output_slots,
                combined_edges,
            ) = self.get_combined_slots_and_edges(combined_implementation, nodes_to_merge)

            # Create new implementation
            new_implementation = Implementation(
                implemented_steps,
                combined_implementation_config,
                combined_input_slots,
                combined_output_slots,
            )
            self.add_node(combined_implementation, implementation=new_implementation)

            # Redirect edges
            for edge in combined_edges:
                self.add_edge_from_params(edge)

            # Remove original nodes
            self.remove_nodes_from(nodes_to_merge)
            try:
                cycle = nx.find_cycle(self)
                raise ValueError("The MultiDiGraph contains a cycle: {}".format(cycle))
            except nx.NetworkXNoCycle:
                pass

    def get_combined_slots_and_edges(
        self, combined_implementation: str, nodes_to_merge: list[str]
    ) -> tuple[set[InputSlot], set[OutputSlot], set[EdgeParams]]:
        slot_types = ["input_slot", "output_slot"]
        combined_slots_by_type = combined_input_slots, combined_output_slots = set(), set()
        edges_by_slot_and_type = self.get_edges_by_slot(nodes_to_merge)
        transform_mappings = (InputSlotMapping, OutputSlotMapping)

        combined_edges = set()

        for slot_type, combined_slots, edges_by_slot, transform_mapping in zip(
            slot_types, combined_slots_by_type, edges_by_slot_and_type, transform_mappings
        ):
            separate_slots = edges_by_slot.keys()
            duplicate_slots = self.get_duplicate_slots(separate_slots, slot_type)
            for slot_tuple in separate_slots:
                step_name, slot = slot_tuple
                edges = edges_by_slot[slot_tuple]
                if slot_tuple in duplicate_slots:
                    combined_slot = slot.__class__(
                        name=step_name + "_" + slot.name,
                        env_var=step_name.upper() + "_" + slot.env_var,
                        validator=slot.validator,
                    )
                else:
                    combined_slot = deepcopy(slot)
                combined_slots.add(combined_slot)
                slot_mapping = transform_mapping(
                    slot.name, combined_implementation, combined_slot.name
                )
                combined_edges.update([slot_mapping.remap_edge(edge) for edge in edges])
        return combined_input_slots, combined_output_slots, combined_edges

    def get_edges_by_slot(
        self,
        nodes_to_merge: list[str],
    ) -> tuple[
        dict[tuple[str, InputSlot], EdgeParams], dict[tuple[str, OutputSlot], EdgeParams]
    ]:

        in_edges_by_slot = defaultdict(list)
        out_edges_by_slot = defaultdict(list)

        # Find separate input and output slots
        for node in nodes_to_merge:
            for pred, succ, data in self.in_edges(node, data=True):
                if pred not in nodes_to_merge:
                    input_slot = data["input_slot"]
                    step_name = self.nodes[node]["implementation"].schema_steps[0]
                    in_edges_by_slot[(step_name, input_slot)].append(
                        EdgeParams.from_graph_edge(pred, succ, data)
                    )

            for pred, succ, data in self.out_edges(node, data=True):

                if succ not in nodes_to_merge:
                    output_slot = data["output_slot"]
                    step_name = self.nodes[node]["implementation"].schema_steps[0]
                    out_edges_by_slot[(step_name, output_slot)].append(
                        EdgeParams.from_graph_edge(pred, succ, data)
                    )
        return in_edges_by_slot, out_edges_by_slot

    def get_duplicate_slots(
        self, slots_tuple: set[tuple[str, InputSlot | OutputSlot]], slot_type: str
    ):
        name_freq = Counter([slot.name for step_name, slot in slots_tuple])
        duplicate_names = [name for name, count in name_freq.items() if count > 1]
        if slot_type == "input_slot":
            env_var_freq = Counter([slot.env_var for step_name, slot in slots_tuple])
            duplicate_env_vars = [
                env_var for env_var, count in env_var_freq.items() if count > 1
            ]
            duplicate_slots = {
                (step_name, slot)
                for (step_name, slot) in slots_tuple
                if slot.name in duplicate_names or slot.env_var in duplicate_env_vars
            }
        else:
            duplicate_slots = {
                (step_name, slot)
                for (step_name, slot) in slots_tuple
                if slot.name in duplicate_names
            }
        return duplicate_slots

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

    def validate_implementation_topology(
        self, nodes: list[str], metadata_steps: list[str]
    ) -> None:
        """Check that the subgraph induced by the nodes implemented by this implementation
        is topologically consistent with the list of metadata steps."""
        subgraph = ImplementationGraph(self).subgraph(nodes)

        # Relabel nodes by schema step
        mapping = {}
        for node, data in subgraph.nodes(data=True):
            schema_steps = data["implementation"].schema_steps
            if len(schema_steps) == 1:
                mapping[node] = schema_steps[0]
            else:
                raise ValueError(
                    f"Node '{node}' must implement exactly one step before combination."
                )
        if not set(mapping.values()) == set(metadata_steps):
            raise ValueError(
                f"Pipeline configuration nodes {list(mapping.values())} do not match metadata steps {metadata_steps}."
            )
        subgraph = nx.relabel_nodes(subgraph, mapping)
        # Check for topological inconsistency, i.e. if there
        # is a path from a later node to an earlier node.
        for i in range(len(metadata_steps)):
            for j in range(i + 1, len(metadata_steps)):
                if nx.has_path(subgraph, metadata_steps[j], metadata_steps[i]):
                    raise ValueError(
                        f"Pipeline configuration nodes {set(subgraph.nodes())} are not topologically consistent with metadata steps {set(metadata_steps)}:"
                        f"There is a path from successor {metadata_steps[j]} to predecessor {metadata_steps[i]}"
                    )
