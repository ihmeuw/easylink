from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable, Optional, Union, Type

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
    source_node_type: Union[Type["Step"], Type[Implementation]]
    target_node: str
    target_node_type: Union[Type["Step"], Type[Implementation]]
    output_slot: str
    input_slot: str
    filepaths: Optional[tuple[str]] = None
    
    
    @classmethod
    def from_graph_edge(cls, source, sink, edge_attrs) -> "Edge":
        return cls(
            source, sink, edge_attrs["output_slot"].name, edge_attrs["input_slot"].name, edge_attrs.get("filepaths")
            )
        
    @property
    def edge_type(self):
        if self.source_node_type == Step and self.target_node_type == Step:
            return "step"
        elif self.source_node_type == Implementation and self.target_node_type == Implementation:
            return "implementation"
        else:
            return "mixed"


class StepGraph(nx.MultiDiGraph):
    def add_node_from_step(self, step: "Step") -> None:
        super().add_node(step.name, step=step)

    def add_edge_from_data(self, edge: Edge) -> None:
        if not edge.edge_type == "step":
            raise ValueError("Edge must connect two steps")
        return super().add_edge(
            edge.source_node,
            edge.target_node,
            output_slot=self.nodes[edge.source_node]["step"].output_slots[edge.output_slot],
            input_slot=self.nodes[edge.target_node]["step"].input_slots[edge.input_slot],
            filepaths=edge.filepaths
        )
        
        
class ImplementationGraph(nx.MultiDiGraph):
    def add_node_from_impl(self, node_name, implementation: Implementation) -> None:
        super().add_node(node_name, implementation=implementation)

    def add_edge_from_data(self, edge: Edge) -> None:
        if not edge.edge_type == "implementation":
            raise ValueError("Edge must connect two implementations")
        return super().add_edge(
            edge.source_node,
            edge.target_node,
            output_slot=edge.output_slot,
            input_slot=edge.input_slot,
            filepaths=edge.filepaths,
        )

@dataclass
class SlotMapping:
    """SlotMapping represents a mapping between a parent and child node
    at different levels of the nested pipeline schema."""

    slot_type: str
    parent_node: str
    parent_node_type: Union[Type[Step], Type[Implementation]]
    parent_slot: str
    child_node: str
    child_node_type: Union[Type[Step], Type[Implementation]]
    child_slot: str

    def propagate_edge(self, edge: Edge) -> Edge:
        if self.slot_type == "input":
            if not edge.target_node == self.parent_node:
                raise ValueError("Parent node does not match target node")
            if not edge.input_slot == self.parent_slot:
                raise ValueError("Parent slot does not match input slot")
            return Edge(
                source_node=edge.source_node,
                source_node_type=edge.source_node_type,
                target_node=self.child_node,
                target_node_type=self.child_node_type,
                output_slot=edge.output_slot,
                input_slot=self.child_slot,
                filepaths=edge.filepaths,
            )
        else:
            if not edge.source_node == self.parent_node:
                raise ValueError("Parent node does not match source node")
            if not edge.output_slot == self.parent_slot:
                raise ValueError("Parent slot does not match output slot")
            return Edge(
                source_node=self.child_node,
                source_node_type=self.child_node_type,
                target_node=edge.target_node,
                target_node_type=edge.target_node_type,
                output_slot=self.child_slot,
                input_slot=edge.input_slot,
                filepaths=edge.filepaths,
            )