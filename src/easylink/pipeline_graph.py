"""
==============
Pipeline Graph
==============

This module is responsible for managing the directed acyclic graph (DAG) of the
entire pipeline to be run from an ``easylink run`` call.

"""

import itertools
from collections import Counter, defaultdict
from copy import deepcopy
from pathlib import Path

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
from easylink.implementation import Implementation, PartialImplementation
from easylink.utilities import paths
from easylink.utilities.data_utils import load_yaml


class PipelineGraph(ImplementationGraph):
    """A directed acyclic graph (DAG) of the pipeline to run.

    The ``PipelineGraph`` is a DAG of the entire pipeline to run. It has
    :class:`Implementations<easylink.implementation.Implementation>` for nodes
    and the file dependencies between them for edges. Multiple edges between nodes
    are permitted.

    This is the highest level type of :class:`~easylink.graph_components.ImplementationGraph`.

    See :class:`~easylink.graph_components.ImplementationGraph` for inherited attributes.

    Parameters
    ----------
    config
        The :class:`~easylink.configuration.Config` object.
    freeze
        Whether to freeze the graph after construction.

    Notes
    -----
    The ``PipelineGraph`` is a low-level abstraction; it represents the *actual
    implementations* of each :class:`~easylink.step.Step` in the resolved pipeline
    being run. This is in contrast to the :class:`~easylink.pipeline_schema.PipelineSchema`,
    which can be an intricate nested structure due to the various complex and self-similar
    ``Step`` instances (which represent abstract operations  such as "loop this
    step N times"). A ``PipelineGraph`` is the flattened and concrete graph of
    ``Implementations`` to run.
    """

    def __init__(self, config: Config, freeze: bool = True) -> None:
        super().__init__(incoming_graph_data=config.schema.get_implementation_graph())
        self._merge_combined_implementations(config)
        self._update_slot_filepaths(config)
        if freeze:
            self = nx.freeze(self)

    @property
    def spark_is_required(self) -> bool:
        """Whether or not any :class:`~easylink.implementation.Implementation` requires spark."""
        return any([implementation.requires_spark for implementation in self.implementations])

    @property
    def any_embarrassingly_parallel(self) -> bool:
        """Whether or not any :class:`~easylink.implementation.Implementation` is
        to be run in an embarrassingly parallel way."""
        return any(
            [
                self.get_whether_embarrassingly_parallel(node)
                for node in self.implementation_nodes
            ]
        )

    def get_whether_embarrassingly_parallel(self, node: str) -> bool:
        """Determines whether a node is to be run in an embarrassingly parallel way.

        Parameters
        ----------
        node
            The node name to determine whether or not it is to be run in an
            embarrassingly parallel way.

        Returns
        -------
            A boolean indicating whether the node is to be run in an embarrassingly
            parallel way.
        """
        return self.nodes[node]["implementation"].is_embarrassingly_parallel

    def get_io_filepaths(self, node: str) -> tuple[list[str], list[str]]:
        """Gets all of a node's input and output filepaths from its edges.

        Parameters
        ----------
        node
            The node name to get input and output filepaths for.

        Returns
        -------
            The input and output filepaths for the node.
        """
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

    def get_io_slot_attributes(
        self, node: str
    ) -> tuple[dict[str, dict[str, str | list[str]]], dict[str, dict[str, str | list[str]]]]:
        """Gets all of a node's i/o slot attributes from edges.

        Parameters
        ----------
        node
            The node name to get slot attributes for.

        Returns
        -------
            A tuple of mappings of node name to slot attributes.
        """
        input_slots = [
            edge_attrs["input_slot"] for _, _, edge_attrs in self.in_edges(node, data=True)
        ]
        input_filepaths_by_slot = [
            list(edge_attrs["filepaths"])
            for _, _, edge_attrs in self.in_edges(node, data=True)
        ]
        input_slot_attrs = self._deduplicate_input_slots(input_slots, input_filepaths_by_slot)

        output_slots = [
            edge_attrs["output_slot"] for _, _, edge_attrs in self.out_edges(node, data=True)
        ]
        output_filepaths_by_slot = [
            list(edge_attrs["filepaths"])
            for _, _, edge_attrs in self.out_edges(node, data=True)
        ]
        output_slot_attrs = self._deduplicate_output_slots(
            output_slots, output_filepaths_by_slot
        )
        return input_slot_attrs, output_slot_attrs

    ##################
    # Helper Methods #
    ##################

    def _merge_combined_implementations(self, config: Config) -> None:
        """Merge nodes with the same combined implementation into a single node.

        It is sometimes useful to use a single :class:`~easylink.implementation.Implementation`
        that implements multiple steps of a pipeline at once. If the user has specified
        such a scenario (via the pipeline configuration), this method will merge
        all nodes that use the same combined implementation into a single node.

        Parameters
        ----------
        config
            The :class:`~easylink.configuration.Config` object.

        Raises
        ------
        ValueError
            If the ``PipelineGraph`` contains a cycle after combining implementations.

        Notes
        -----
        There are no special class abstractions to represent combined implementations.
        Rather, the merging of nodes is explicitly called directly on the original
        ``PipelineGraph``.
        """
        implementation_metadata = load_yaml(paths.IMPLEMENTATION_METADATA)
        for (
            combined_implementation,
            combined_implementation_config,
        ) in config.pipeline.combined_implementations.items():

            # Find all nodes with the same combined implementation
            nodes_to_merge = [
                node
                for node, data in self.nodes(data=True)
                if isinstance(data["implementation"], PartialImplementation)
                and data["implementation"].combined_name == combined_implementation
            ]
            if not nodes_to_merge:
                continue

            partial_implementations_to_merge = [
                self.nodes[node]["implementation"] for node in nodes_to_merge
            ]

            implemented_steps = [
                implementation.schema_step
                for implementation in partial_implementations_to_merge
            ]

            metadata_steps = implementation_metadata[combined_implementation_config["name"]][
                "steps"
            ]
            self._validate_combined_implementation_topology(nodes_to_merge, metadata_steps)

            (
                combined_input_slots,
                combined_output_slots,
                combined_edges,
            ) = self._get_combined_slots_and_edges(combined_implementation, nodes_to_merge)

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
                raise ValueError(
                    "The pipeline graph contains a cycle after combining "
                    "implementations: {}".format(cycle)
                )
            except nx.NetworkXNoCycle:
                pass

    def _validate_combined_implementation_topology(
        self, nodes: list[str], metadata_steps: list[str]
    ) -> None:
        """Validates the combined implementation topology against intended implementation.

        Parameters
        ----------
        nodes
            Sorted names of the nodes to validate.
        metadata_steps
            Sorted names of the steps that the nodes are intended to implement.

        Raises
        ------
        ValueError
            If there is a mismatch between the nodes and the steps they intend
            to implement or if the nodes are no longer topologically consistent.
        """
        # HACK: We cannot just call self.subgraph(nodes) because networkx will
        # try and instantiate another PipelineGraph which requires a Config object
        # to be passed to the constructor.
        subgraph = ImplementationGraph(self).subgraph(nodes)

        # Relabel nodes by schema step
        mapping = {
            node: data["implementation"].schema_step
            for node, data in subgraph.nodes(data=True)
        }
        if not set(mapping.values()) == set(metadata_steps):
            # NOTE: It's possible that we've combined nodes in such a way that removed
            # an edge from the graph and so nx is unable to reliably sort the subgraph.
            full_pipeline_sorted_nodes = list(nx.topological_sort(self))
            sorted_nodes = [node for node in full_pipeline_sorted_nodes if node in mapping]
            raise ValueError(
                f"Pipeline configuration nodes {[mapping[node] for node in sorted_nodes]} "
                f"do not match metadata steps {metadata_steps}."
            )
        subgraph = nx.relabel_nodes(subgraph, mapping)
        # Check for topological inconsistency, i.e. if there is a path from a later
        # node to an earlier node.
        for predecessor in range(len(metadata_steps)):
            for successor in range(predecessor + 1, len(metadata_steps)):
                if nx.has_path(
                    subgraph, metadata_steps[successor], metadata_steps[predecessor]
                ):
                    raise ValueError(
                        f"Pipeline configuration nodes {list(nx.topological_sort(subgraph))} "
                        "are not topologically consistent with the intended implementations "
                        f"for {list(metadata_steps)}:\n"
                        f"There is a path from successor {metadata_steps[successor]} "
                        f"to predecessor {metadata_steps[predecessor]}."
                    )

    def _get_combined_slots_and_edges(
        self, combined_implementation: str, nodes_to_merge: list[str]
    ) -> tuple[set[InputSlot], set[OutputSlot], set[EdgeParams]]:
        """Gets all edge information required for the combined implementation.

        Parameters
        ----------
        combined_implementation
            The name of the combined implementation.
        nodes_to_merge
            The names of the nodes being merged.

        Returns
        -------
            The :class:`InputSlots<easylink.graph_components.InputSlot>`,
            :class:`OutputSlots<easylink.graph_components.OutputSlot>`, and
            :class:`~easylink.graph_components.EdgeParams` needed to construct the
            combined implementation.

        Notes
        -----
        When combining implementations results in a node with multiple slots with
        the same name and/or environment variable, the slots are made unique
        by prepending the :class:`~easylink.step.Step` name to the slot name as well
        as to the environment variable. This is necessary to prevent collisions
        with a combined implementation that takes multiple environment variables that
        have the same name.
        """
        slot_types = ["input_slot", "output_slot"]
        combined_slots_by_type = combined_input_slots, combined_output_slots = set(), set()
        edges_by_slot_and_type = self._get_edges_by_slot(nodes_to_merge)
        transform_mappings = (InputSlotMapping, OutputSlotMapping)

        combined_edges = set()
        # FIXME [MIC-5848]: test coverage is lacking when two output slots have the same name,
        # i.e. combing two steps that have the same name output slots
        for slot_type, combined_slots, edges_by_slot, transform_mapping in zip(
            slot_types, combined_slots_by_type, edges_by_slot_and_type, transform_mappings
        ):
            separate_slots = edges_by_slot.keys()
            duplicate_slots = self._get_duplicate_slots(separate_slots, slot_type)
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

    def _get_edges_by_slot(
        self,
        nodes_to_merge: list[str],
    ) -> tuple[
        dict[tuple[str, InputSlot], EdgeParams], dict[tuple[str, OutputSlot], EdgeParams]
    ]:
        """Gets the edge information for all nodes to be combined.

        Parameters
        ----------
        nodes_to_merge
            A list of ``PipelineGraph`` nodes to be combined.

        Returns
        -------
            Input and output slot-edge mappings.
        """

        in_edges_by_slot = defaultdict(list)
        out_edges_by_slot = defaultdict(list)

        # Find separate input and output slots
        for node in nodes_to_merge:
            for pred, succ, data in self.in_edges(node, data=True):
                if pred not in nodes_to_merge:
                    input_slot = data["input_slot"]
                    partial_implementation = self.nodes[node]["implementation"]
                    in_edges_by_slot[(partial_implementation.schema_step, input_slot)].append(
                        EdgeParams.from_graph_edge(pred, succ, data)
                    )

            for pred, succ, data in self.out_edges(node, data=True):

                if succ not in nodes_to_merge:
                    output_slot = data["output_slot"]
                    partial_implementation = self.nodes[node]["implementation"]
                    out_edges_by_slot[
                        (partial_implementation.schema_step, output_slot)
                    ].append(EdgeParams.from_graph_edge(pred, succ, data))
        return in_edges_by_slot, out_edges_by_slot

    @staticmethod
    def _get_duplicate_slots(
        slot_tuples: set[tuple[str, InputSlot | OutputSlot]], slot_type: str
    ) -> set[tuple[str, InputSlot | OutputSlot]]:
        """Gets duplicate slots.

        Combining nodes can lead to duplicate slots. In order to deduplicate them,
        this helper method returns only those which have duplicate names or
        environment variables.

        Parameters
        ----------
        slot_tuples
            A set of (step_name, slot) tuples to check for duplication.
        slot_type
            "input_slot" or "output_slot"

        Returns
        -------
            A set of (step_name, slot) tuples that have duplicate names or environment
            variables to be deduplicated.
        """
        name_freq = Counter([slot.name for step_name, slot in slot_tuples])
        duplicate_names = [name for name, count in name_freq.items() if count > 1]
        if slot_type == "input_slot":
            env_var_freq = Counter([slot.env_var for step_name, slot in slot_tuples])
            duplicate_env_vars = [
                env_var for env_var, count in env_var_freq.items() if count > 1
            ]
            duplicate_slots = {
                (step_name, slot)
                for (step_name, slot) in slot_tuples
                if slot.name in duplicate_names or slot.env_var in duplicate_env_vars
            }
        else:
            duplicate_slots = {
                (step_name, slot)
                for (step_name, slot) in slot_tuples
                if slot.name in duplicate_names
            }
        return duplicate_slots

    def _update_slot_filepaths(self, config: Config) -> None:
        """Fills graph edges with appropriate filepath information.

        This method updates the :class:`~easylink.step.Step` slot information with
        actual filepaths. This can't happen earlier in the process because we
        don't know node names until now (which are required for the filepaths).

        Parameters
        ----------
        config
            The :class:`~easylink.configuration.Config`.
        """
        # Update input data edges to correct filenames from config
        for src, sink, edge_attrs in self.out_edges("input_data", data=True):
            for edge_idx in self[src][sink]:
                if edge_attrs["output_slot"].name == "all":
                    self[src][sink][edge_idx]["filepaths"] = tuple(
                        str(path) for path in config.input_data.to_dict().values()
                    )
                else:
                    self[src][sink][edge_idx]["filepaths"] = (
                        str(config.input_data[edge_attrs["output_slot"].name]),
                    )

        # Update implementation nodes with yaml metadata
        for node in self.implementation_nodes:
            implementation = self.nodes[node]["implementation"]
            imp_outputs = implementation.outputs
            for src, sink, edge_attrs in self.out_edges(node, data=True):
                for edge_idx in self[node][sink]:
                    self[src][sink][edge_idx]["filepaths"] = (
                        str(
                            Path("intermediate")
                            / node
                            / imp_outputs[edge_attrs["output_slot"].name]
                        ),
                    )

    @staticmethod
    def _deduplicate_input_slots(
        input_slots: list[InputSlot], filepaths_by_slot: list[str]
    ) -> dict[str, dict[str, str | list[str]]]:
        """Deduplicates input slots into a dictionary with filepaths.

        Parameters
        ----------
        input_slots
            The :class:`InputSlots<easylink.graph_components.InputSlot>` condense.
        filepaths_by_slot
            The filepaths associated with each ``InputSlot``.

        Returns
        -------
            A dictionary mapping ``InputSlot`` names to their attributes and filepaths.

        Raises
        ------
        ValueError
            If duplicate slot names are found with different environment variables
            or validators.
        """
        condensed_slot_dict = {}
        for input_slot, filepaths in zip(input_slots, filepaths_by_slot):
            slot_name, env_var, validator, splitter = (
                input_slot.name,
                input_slot.env_var,
                input_slot.validator,
                input_slot.splitter,
            )
            if slot_name in condensed_slot_dict:
                if env_var != condensed_slot_dict[slot_name]["env_var"]:
                    raise ValueError(
                        f"Duplicate input slots named '{slot_name}' have different env vars."
                    )
                condensed_slot_validator = condensed_slot_dict[slot_name]["validator"]
                if validator != condensed_slot_validator:
                    raise ValueError(
                        f"Duplicate input slots named '{slot_name}' have different validators: "
                        f"'{validator.__name__}' and '{condensed_slot_validator.__name__}'."
                    )
                # Add the new filepaths to the existing slot
                condensed_slot_dict[slot_name]["filepaths"].extend(filepaths)
            else:
                condensed_slot_dict[slot_name] = {
                    "env_var": env_var,
                    "validator": validator,
                    "filepaths": filepaths,
                    "splitter": splitter,
                }
        return condensed_slot_dict

    @staticmethod
    def _deduplicate_output_slots(
        output_slots: list[OutputSlot], filepaths_by_slot: list[str]
    ) -> dict[str, dict[str, str | list[str]]]:
        """Deduplicates output slots into a dictionary with filepaths.

        Parameters
        ----------
        output_slots
            The :class:`OutputSlots<easylink.graph_components.OutputSlot>` to deduplicate.
        filepaths_by_slot
            The filepaths associated with each ``OutputSlot``.

        Returns
        -------
            A dictionary mapping ``OutputSlot`` names to their attributes and filepaths.
        """
        condensed_slot_dict = {}
        for output_slot, filepaths in zip(output_slots, filepaths_by_slot):
            slot_name, aggregator = (
                output_slot.name,
                output_slot.aggregator,
            )
            if slot_name in condensed_slot_dict:
                # Add the new filepaths to the existing slot
                condensed_slot_dict[slot_name]["filepaths"].extend(filepaths)
            else:
                condensed_slot_dict[slot_name] = {
                    "filepaths": filepaths,
                    "aggregator": aggregator,
                }
        return condensed_slot_dict
