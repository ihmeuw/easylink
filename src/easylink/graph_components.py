from dataclasses import dataclass
from typing import Callable, Optional, TYPE_CHECKING
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
@dataclass Edge:
    source_node: Step | Implementation
    target_node: Step | Implementation
    output_slot: OutputSlot
    input_slot: InputSlot
    
@dataclass
class StepGraphEdge:
    """An edge between two nodes in a graph. Edges connect the output slot of
    the source node to the input slot of the target node."""

    source_node: Step
    target_node: Step
    output_slot: OutputSlot
    input_slot: InputSlot

    @classmethod
    def from_graph_edge(cls, source, sink, edge_attrs) -> "Edge":
        return cls(source, sink, edge_attrs["output_slot"], edge_attrs["input_slot"])
class StepGraph(nx.MultiDiGraph):
    def add_node(self, node: "Step") -> None:
        super().add_node(node.step_name, step=node)
    
    def add_edge(self, edge: StepGraphEdge) -> None:
        return super().add_edge(edge.source_node, edge.target_node, output_slot=self.nodes[edge.source_node]["step"].output_slots[edge.output_slot], 
                                input_slot=self.nodes[edge.target_node]["step"].input_slots[edge.input_slot])
    
@dataclass
class ImplementationGraphEdge:
    """An edge between two nodes in a graph. Edges connect the output slot of
    the source node to the input slot of the target node."""

    source_node: Implementation
    target_node: Implementation
    output_slot: OutputSlot
    input_slot: InputSlot
    filepaths: Optional[tuple[str]] = None

    @classmethod
    def from_graph_edge(cls, source, sink, edge_attrs) -> "Edge":
        return cls(source, sink, edge_attrs["output_slot"], edge_attrs["input_slot"])

class ImplementationGraph(nx.MultiDiGraph):
    def add_node(self, node_name, implementation: Implementation) -> None:
        super().add_node(node_name, implementation=implementation)
    
    def add_edge(self, edge: ImplementationGraphEdge) -> None:
        return super().add_edge(edge.source_node, edge.target_node, output_slot=edge.output_slot, input_slot=edge.input_slot, filepaths=edge.filepaths)

@dataclass
class SlotMapping:
    """SlotMapping represents a mapping between a parent and child node
    at different levels of the nested pipeline schema."""

    slot_type: str
    parent_node: str
    parent_slot: str
    child_node: str
    child_slot: str

class StepSlotMapping(SlotMapping):
    
    def propagate_edge(self, edge: StepGraphEdge) -> StepGraphEdge:
        if self.slot_type == "input":
            if not edge.target_node == self.parent_node:
                raise ValueError("Parent node does not match target node")
            if not edge.input_slot == self.parent_slot:
                raise ValueError("Parent slot does not match input slot")
            return StepGraphEdge(
                source_node=edge.source_node,
                target_node=self.child_node,
                output_slot=edge.output_slot,
                input_slot=self.child_slot
            )
        else:
            if not edge.source_node == self.parent_node:
                raise ValueError("Parent node does not match source node")
            if not edge.output_slot == self.parent_slot:
                raise ValueError("Parent slot does not match output slot")
            return StepGraphEdge(
                source_node=self.child_node,
                target_node=edge.target_node,
                output_slot=self.child_slot,
                input_slot=edge.input_slot
            )

class ImplementationSlotMapping(SlotMapping):
        
        def propagate_edge(self, edge: StepGraphEdge) -> ImplementationGraphEdge:
            if self.slot_type == "input":
                if not edge.target_node == self.parent_node:
                    raise ValueError("Parent node does not match target node")
                if not edge.input_slot.name == self.parent_slot:
                    raise ValueError("Parent slot does not match input slot")
                return ImplementationGraphEdge(
                    source_node=edge.source_node,
                    target_node=self.child_node,
                    output_slot=edge.output_slot,
                    input_slot=self.child_slot
                )
            else:
                if not edge.source_node == self.parent_node:
                    raise ValueError("Parent node does not match source node")
                if not edge.output_slot.name == self.parent_slot:
                    raise ValueError("Parent slot does not match output slot")
                return ImplementationGraphEdge(
                    source_node=self.child_node,
                    target_node=edge.target_node,
                    output_slot=self.child_slot,
                    input_slot=edge.input_slot
                )
    
