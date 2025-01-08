"""
================
Graph Components
================

This module is responsible for defining the modular building-block objects that 
can be composed to create graph representations of pipelines.

"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

import networkx as nx

if TYPE_CHECKING:
    from easylink.implementation import Implementation
    from easylink.step import Step


@dataclass(frozen=True)
class InputSlot:
    """An abstraction representing a single input slot to a specific node.

    ``InputSlots`` represent distinct semantic categories of input files, between
    which a node must be able to differentiate. In order to pass data between nodes,
    an ``InputSlot`` of one node can be connected to an :class:`OutputSlot` of another
    node via an :class:`EdgeParams` instance.

    Notes
    -----
    Nodes can be either :class:`Steps<easylink.step.Step>` or :class:`Implementations<easylink.implementation.Implementation>`.
    """

    name: str
    """The name of the ``InputSlot``."""
    env_var: str | None
    """The environment variable that this ``InputSlot`` will use to pass a list 
    of data filepaths to an ``Implementation``."""
    validator: Callable[[str], None]
    """A callable that validates the input data being passed into the pipeline via 
    this ``InputSlot``. If the data is invalid, the callable should raise an exception 
    with a descriptive error message which will then be reported to the user."""


@dataclass(frozen=True)
class OutputSlot:
    """An abstraction representing a single output slot from a specific node.

    In order to pass data between nodes, an ``OutputSlot`` of one node can be connected
    to an :class:`InputSlot` of another node via an :class:`EdgeParams` instance.

    Notes
    -----
    Nodes can be either :class:`Steps<easylink.step.Step>` or :class:`Implementations<easylink.implementation.Implementation>`.

    Input data is validated via the ``InputSlot`` required :attr:`~InputSlot.validator`
    attribute. In order to prevent multiple validations of the same files (since
    outputs of one node can be inputs to another), no such validator is stored
    here on the ``OutputSlot``.
    """

    name: str
    """The name of the ``OutputSlot``."""


@dataclass(frozen=True)
class EdgeParams:
    """A representation of an edge between two nodes in a graph.

    EdgeParams connect the :class:`OutputSlot` of a source node to the :class:`InputSlot`
    of a target node.

    Notes
    -----
    Nodes can be either :class:`Steps<easylink.step.Step>` or :class:`Implementations<easylink.implementation.Implementation>`.
    """

    source_node: str
    """The name of the source node/``Step``."""
    target_node: str
    """The name of the target node/``Step``."""
    output_slot: str
    """The name of the ``OutputSlot`` of the source node/``Step``."""
    input_slot: str
    """The name of the ``InputSlot`` of the target node/``Step``."""
    filepaths: tuple[str] | None = None
    """The filepaths that are passed from the source node/``Step`` to the target node/``Step``."""

    @classmethod
    def from_graph_edge(
        cls: type["EdgeParams"],
        source: str,
        sink: str,
        edge_attrs: dict[str, OutputSlot | InputSlot | str | None],
    ) -> EdgeParams:
        """A convenience method to create an ``EdgeParams`` instance.

        Parameters
        ----------
        source
            The name of the source node/``Step``.
        sink
            The name of the target node/``Step``.
        edge_attrs
            The attributes of the edge connecting the source and target nodes/``Steps``.
            'output_slot' and 'input_slot' are required keys and 'filepaths' is optional.
        """
        return cls(
            source,
            sink,
            edge_attrs["output_slot"].name,
            edge_attrs["input_slot"].name,
            edge_attrs.get("filepaths"),
        )


class StepGraph(nx.MultiDiGraph):
    """A directed acyclic graph (DAG) of :class:`Steps<easylink.step.Step>` and the data dependencies between them.

    ``StepGraphs`` are DAGs with ``Steps`` for nodes and the file dependencies between
    them for edges. Multiple edges between nodes are permitted.

    Notes
    -----
    These are high-level abstractions; they represent a conceptual pipeline
    graph with no detail as to how each ``Step`` is implemented.

    The highest level ``StepGraph`` is the that of the entire :class:`~easylink.pipeline_schema.PipelineSchema`.
    """

    @property
    def step_nodes(self) -> list[str]:
        """The topologically sorted list of node/``Step`` names."""
        ordered_nodes = list(nx.topological_sort(self))
        return [node for node in ordered_nodes if node != "input_data" and node != "results"]

    @property
    def steps(self) -> list[Step]:
        """The list of all ``Steps`` in the graph."""
        return [self.nodes[node]["step"] for node in self.step_nodes]

    def add_node_from_step(self, step: Step) -> None:
        """Adds a new node to the ``StepGraph``.

        Parameters
        ----------
        step
            The ``Step`` to add to the graph as a new node.
        """
        self.add_node(step.name, step=step)

    def add_edge_from_params(self, edge_params: EdgeParams) -> None:
        """Adds a new edge to the ``StepGraph``.

        Parameters
        ----------
        edge_params
            The details of the new edge to be added to the graph.
        """
        return self.add_edge(
            edge_params.source_node,
            edge_params.target_node,
            output_slot=self.nodes[edge_params.source_node]["step"].output_slots[
                edge_params.output_slot
            ],
            input_slot=self.nodes[edge_params.target_node]["step"].input_slots[
                edge_params.input_slot
            ],
        )


class ImplementationGraph(nx.MultiDiGraph):
    """A directed acyclic graph (DAG) of :class:`Implementations<easylink.implementation.Implementation>`.

    ``ImplementationGraphs`` are DAGs with ``Implementations`` for nodes and the
    file dependencies between them for edges. Self-edges as well as multiple edges
    between nodes are permitted.

    Notes
    -----
    An ``ImplementationGraph`` is a low-level abstraction; it represents the *actual
    implementations* of each :class:`~easylink.step.Step` in the pipeline. This
    is in contrast to a :class:`StepGraph`, which can be an intricate nested structure
    due to the various complex and self-similar ``Step`` instances (which represent
    abstract operations such as "loop this step N times"). An ``ImplementationGraph``
    is the flattened and concrete graph of ``Implementations`` to run.

    The highest level ``ImplementationGraph`` is the that of the entire
    :class:`~easylink.pipeline_graph.PipelineGraph`.
    """

    @property
    def implementation_nodes(self) -> list[str]:
        """The topologically sorted list of node/``Implementation`` names."""
        ordered_nodes = list(nx.topological_sort(self))
        return [node for node in ordered_nodes if node != "input_data" and node != "results"]

    @property
    def implementations(self) -> list[Implementation]:
        """The list of all ``Implementations`` in the graph."""
        return [self.nodes[node]["implementation"] for node in self.implementation_nodes]

    def add_node_from_implementation(self, node_name, implementation: Implementation) -> None:
        """Adds a new node to the ``ImplementationGraph``.

        Parameters
        ----------
        node_name
            The name of the new node.
        implementation
            The ``Implementation`` to add to the graph as a new node.
        """
        self.add_node(node_name, implementation=implementation)

    def add_edge_from_params(self, edge_params: EdgeParams) -> None:
        """Adds a new edge to the ``ImplementationGraph``.

        Parameters
        ----------
        edge_params
            The details of the new edge to be added to the graph.
        """
        return self.add_edge(
            edge_params.source_node,
            edge_params.target_node,
            output_slot=self.nodes[edge_params.source_node]["implementation"].output_slots[
                edge_params.output_slot
            ],
            input_slot=self.nodes[edge_params.target_node]["implementation"].input_slots[
                edge_params.input_slot
            ],
            filepaths=edge_params.filepaths,
        )


@dataclass(frozen=True)
class SlotMapping(ABC):
    """A mapping between a slot on a parent :class:`~easylink.step.Step` and a slot on one of its child ``Steps``.

    ``SlotMapping`` is an interface intended to be used by concrete :class:`InputSlotMapping`
    and :class:`OutputSlotMapping` classes. It represents a mapping between
    parent and child nodes/``Steps`` at different levels of a potentially-nested
    :class:`~easylink.pipeline_schema.PipelineSchema`.
    """

    parent_slot: str
    """The name of the parent slot."""
    child_node: str
    """The name of the child node/``Step``."""
    child_slot: str
    """The name of the child slot."""

    @abstractmethod
    def remap_edge(self, edge: EdgeParams) -> EdgeParams:
        """Remaps an edge to connect the parent and child nodes/``Steps``."""
        pass


class InputSlotMapping(SlotMapping):
    """A mapping between :class:`InputSlots<InputSlot>` of a parent node/:class:`~easylink.step.Step` and a child node/``Step``."""

    def remap_edge(self, edge: EdgeParams) -> EdgeParams:
        """Remaps an edge's ``InputSlot``.

        Parameters
        ----------
        edge
            The edge to remap.

        Returns
        -------
        EdgeParams
            The details of the remapped edge.

        Raises
        ------
        ValueError
            If the parent slot does not match the input slot of the edge.
        """
        if edge.input_slot != self.parent_slot:
            raise ValueError("Parent slot does not match input slot")
        return EdgeParams(
            source_node=edge.source_node,
            target_node=self.child_node,
            output_slot=edge.output_slot,
            input_slot=self.child_slot,
        )


class OutputSlotMapping(SlotMapping):
    """A mapping between :class:`InputSlots<InputSlot>` of a parent node/:class:`~easylink.step.Step` and a child node/``Step``."""

    def remap_edge(self, edge: EdgeParams) -> EdgeParams:
        """Remaps an edge's :class:`OutputSlot`.

        Parameters
        ----------
        edge
            The edge to remap.

        Returns
        -------
        EdgeParams
            The details of the remapped edge.

        Raises
        ------
        ValueError
            If the parent slot does not match the output slot of the edge.
        """
        if edge.output_slot != self.parent_slot:
            raise ValueError("Parent slot does not match output slot")
        return EdgeParams(
            source_node=self.child_node,
            target_node=edge.target_node,
            output_slot=self.child_slot,
            input_slot=edge.input_slot,
        )
