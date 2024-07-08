from dataclasses import dataclass
from typing import Callable, Optional


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


@dataclass
class SlotMapping:
    """SlotMapping represents a mapping between a parent and child node
    at different levels of the nested pipeline schema."""

    slot_type: str
    parent_node: str
    parent_slot: str
    child_node: str
    child_slot: str
