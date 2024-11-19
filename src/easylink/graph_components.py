from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Optional

import networkx as nx

if TYPE_CHECKING:
    from easylink.implementation import Implementation
    from easylink.step import Step


@dataclass(frozen=True)
class InputSlot:
    """InputSlot  represents a single input slot for a step."""

    name: str
    env_var: Optional[str]
    validator: Callable


@dataclass(frozen=True)
class OutputSlot:
    """OutputSlot  represents a single output slot for a step."""

    name: str


@dataclass(frozen=True)
class EdgeParams:
    """A dataclass representation of an edge between two nodes in a networkx graph.
    Edges connect the output slot of the source node to the input slot of the target node."""

    source_node: str
    target_node: str
    output_slot: str
    input_slot: str
    filepaths: Optional[tuple[str]] = None

    @classmethod
    def from_graph_edge(cls, source, sink, edge_attrs) -> EdgeParams:
        return cls(
            source,
            sink,
            edge_attrs["output_slot"].name,
            edge_attrs["input_slot"].name,
            edge_attrs.get("filepaths"),
        )


class StepGraph(nx.MultiDiGraph):
    """A graph of Steps, with edges representing file dependencies between them.
    StepGraphs are contained as an attribute of Steps, with the highest level being
    the PipelineSchema."""

    @property
    def step_nodes(self) -> list[str]:
        """Return list of nodes tied to specific steps."""
        ordered_nodes = list(nx.topological_sort(self))
        return [node for node in ordered_nodes if node != "input_data" and node != "results"]

    @property
    def steps(self) -> list[Step]:
        """Convenience property to get all steps in the graph."""
        return [self.nodes[node]["step"] for node in self.step_nodes]

    def add_node_from_step(self, step: Step) -> None:
        self.add_node(step.name, step=step)

    def add_edge_from_params(self, edge_params: EdgeParams) -> None:
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
    """A graph of Implementations, with edges representing file dependencies between them.
    ImplementationGraphs are subgraphs of a PipelineGraph generated by a particular Step,
    including the PipelineGraph itself."""

    @property
    def implementation_nodes(self) -> list[str]:
        """Return list of nodes tied to specific implementations."""
        ordered_nodes = list(nx.topological_sort(self))
        return [node for node in ordered_nodes if node != "input_data" and node != "results"]

    @property
    def implementations(self) -> list[Implementation]:
        """Convenience property to get all implementations in the graph."""
        return [self.nodes[node]["implementation"] for node in self.implementation_nodes]

    def add_node_from_implementation(self, node_name, implementation: Implementation) -> None:
        self.add_node(node_name, implementation=implementation)

    def add_edge_from_params(self, edge_params: EdgeParams) -> None:
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
    """SlotMapping represents a mapping between a parent and child node
    at different levels of the nested pipeline schema."""

    parent_slot: str
    child_node: str
    child_slot: str

    @abstractmethod
    def remap_edge(self, edge: EdgeParams) -> EdgeParams:
        pass


class InputSlotMapping(SlotMapping):
    def remap_edge(self, edge: EdgeParams) -> EdgeParams:
        if edge.input_slot != self.parent_slot:
            raise ValueError("Parent slot does not match input slot")
        return EdgeParams(
            source_node=edge.source_node,
            target_node=self.child_node,
            output_slot=edge.output_slot,
            input_slot=self.child_slot,
        )


class OutputSlotMapping(SlotMapping):
    def remap_edge(self, edge: EdgeParams) -> EdgeParams:
        if edge.output_slot != self.parent_slot:
            raise ValueError("Parent slot does not match output slot")
        return EdgeParams(
            source_node=self.child_node,
            target_node=edge.target_node,
            output_slot=self.child_slot,
            input_slot=edge.input_slot,
        )
