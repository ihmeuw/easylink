from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Optional

import networkx as nx

from easylink.implementation import Implementation

if TYPE_CHECKING:
    from easylink.step import Step


@dataclass
class InputSlot:
    """InputSlot  represents a single input slot for a step."""

    name: str
    env_var: Optional[str]
    validator: Callable


@dataclass
class OutputSlot:
    """OutputSlot  represents a single output slot for a step."""

    name: str


@dataclass
class Edge:
    """An edge between two nodes in a graph. Edges connect the output slot of
    the source node to the input slot of the target node."""

    source_node: str
    target_node: str
    output_slot: str
    input_slot: str
    filepaths: Optional[tuple[str]] = None

    @classmethod
    def from_graph_edge(cls, source, sink, edge_attrs) -> "Edge":
        return cls(
            source,
            sink,
            edge_attrs["output_slot"].name,
            edge_attrs["input_slot"].name,
            edge_attrs.get("filepaths"),
        )


class StepGraph(nx.MultiDiGraph):
    def add_node_from_step(self, step: "Step") -> None:
        super().add_node(step.name, step=step)

    def add_edge_from_data(self, edge: Edge) -> None:
        return super().add_edge(
            edge.source_node,
            edge.target_node,
            output_slot=self.nodes[edge.source_node]["step"].output_slots[edge.output_slot],
            input_slot=self.nodes[edge.target_node]["step"].input_slots[edge.input_slot],
        )


class ImplementationGraph(nx.MultiDiGraph):
    def add_node_from_impl(self, node_name, implementation: Implementation) -> None:
        super().add_node(node_name, implementation=implementation)

    def add_edge_from_data(self, edge: Edge) -> None:
        return super().add_edge(
            edge.source_node,
            edge.target_node,
            output_slot=self.nodes[edge.source_node]["implementation"].output_slots[
                edge.output_slot
            ],
            input_slot=self.nodes[edge.target_node]["implementation"].input_slots[
                edge.input_slot
            ],
            filepaths=edge.filepaths,
        )


@dataclass
class SlotMapping:
    """SlotMapping represents a mapping between a parent and child node
    at different levels of the nested pipeline schema."""

    parent_slot: str
    child_node: str
    child_slot: str

    @abstractmethod
    def propagate_edge(self, edge: Edge) -> Edge:
        pass


class InputSlotMapping(SlotMapping):
    def propagate_edge(self, edge: Edge) -> Edge:
        if edge.input_slot != self.parent_slot:
            raise ValueError("Parent slot does not match input slot")
        return Edge(
            source_node=edge.source_node,
            target_node=self.child_node,
            output_slot=edge.output_slot,
            input_slot=self.child_slot,
        )


class OutputSlotMapping(SlotMapping):
    def propagate_edge(self, edge: Edge) -> Edge:
        if edge.output_slot != self.parent_slot:
            raise ValueError("Parent slot does not match output slot")
        return Edge(
            source_node=self.child_node,
            target_node=edge.target_node,
            output_slot=self.child_slot,
            input_slot=edge.input_slot,
        )
