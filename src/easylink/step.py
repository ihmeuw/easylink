from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Tuple

from layered_config_tree import LayeredConfigTree
from networkx import MultiDiGraph

from easylink.implementation import Implementation

if TYPE_CHECKING:
    from easylink.configuration import Config


@dataclass
class InputSlot:
    """StepInput is a dataclass that represents a single input slot for a step."""

    name: str
    env_var: Optional[str]
    validator: Callable


class Step(ABC):
    """Steps contain information about the purpose of the interoperable elements of
    the sequence called a *Pipeline* and how those elements relate to one another.
    In turn, steps are implemented by Implementations, such that each step may have
    several implementations but each implementation must have exactly one step.
    In a sense, steps contain metadata about the implementations to which they relate.
    """

    def __init__(
        self, name: str, input_slots: List[Tuple[str]] = [], output_slots: List[str] = []
    ) -> None:
        self.name = name
        self.input_slots = {slot[0]: InputSlot(*slot) for slot in input_slots}
        self.output_slots = output_slots

    @abstractmethod
    def get_implementation_graph(self, graph, pipeline_config) -> MultiDiGraph:
        """Resolve the Step into an Implementation graph."""
        pass

    @abstractmethod
    def update_edges(self, graph, pipeline_config: LayeredConfigTree) -> None:
        """Update the edges of the graph."""
        pass


class CompositeStep(Step):
    """Composite Steps are Steps that contain other Steps. They allow operations to be
    applied recursively on Steps or sequences of Steps."""

    def __init__(
        self,
        name: str,
        input_slots: List[Tuple[str]] = [],
        output_slots: List[str] = [],
        nodes: List[Step] = [],
        edges: List[Tuple[str]] = [],
        slot_mappings: Dict[str, List[Tuple[str]]] = {"input": [], "output": []},
    ) -> None:
        super().__init__(name, input_slots, output_slots)
        self.graph = self._create_graph(nodes, edges)
        self.slot_mappings = slot_mappings

    def _create_graph(self, nodes, edges) -> MultiDiGraph:
        """Create a MultiDiGraph from the parameters passed in."""
        graph = MultiDiGraph()
        for step in nodes:
            graph.add_node(
                step.name,
                step=step,
            )
        for in_node, out_node, output_slot, input_slot in edges:
            graph.add_edge(
                in_node,
                out_node,
                input_slot=graph.nodes[out_node]["step"].input_slots[input_slot],
                output_slot=output_slot,
            )

        return graph

    def get_sub_config(self, pipeline_config: LayeredConfigTree) -> LayeredConfigTree:
        """Return the subconfig for this step."""
        return pipeline_config[self.name]

    def get_implementation_graph(
        self, graph, pipeline_config: LayeredConfigTree
    ) -> MultiDiGraph:
        """Call get_subgraph on each subgraph node and update the graph."""
        graph.update(self.graph)
        self.remap_slots(graph, pipeline_config)
        sub_config = self.get_sub_config(pipeline_config)
        for node in self.graph.nodes:
            step = self.graph.nodes[node]["step"]
            step.get_implementation_graph(graph, sub_config)
            step.update_edges(graph, sub_config)
            graph.remove_node(node)

    def update_edges(self, graph, pipeline_config: LayeredConfigTree) -> None:
        pass

    def remap_slots(self, graph, pipeline_config: LayeredConfigTree) -> None:
        for child_node, parent_slot, child_slot in self.slot_mappings["input"]:
            parent_edge = [
                (u, v, edge_attrs)
                for (u, v, edge_attrs) in graph.in_edges(self.name, data=True)
                if edge_attrs.get("input_slot").name == parent_slot
            ]
            if not parent_edge:
                raise ValueError(f"Edge not found for {self.name} input slot {parent_slot}")
            if len(parent_edge) > 1:
                raise ValueError(
                    f"Multiple edges found for {self.name} input slot {parent_slot}"
                )
            source, _, edge_attrs = parent_edge[0]
            graph.add_edge(
                source,
                child_node,
                input_slot=graph.nodes[child_node]["step"].input_slots[child_slot],
                output_slot=edge_attrs["output_slot"],
            )

        for child_node, parent_slot, child_slot in self.slot_mappings["output"]:
            parent_edges = [
                (u, v, edge_attrs)
                for (u, v, edge_attrs) in graph.out_edges(self.name, data=True)
                if edge_attrs.get("output_slot") == parent_slot
            ]
            for _, sink, edge_attrs in parent_edges:
                graph.add_edge(
                    child_node,
                    sink,
                    input_slot=edge_attrs["input_slot"],
                    output_slot=child_slot,
                )


class InputStep(Step):
    """Basic Step for input data node."""

    def get_implementation_graph(
        self, graph, pipeline_config: LayeredConfigTree
    ) -> MultiDiGraph:
        graph.add_node("input_data")

    def update_edges(self, graph, pipeline_config: LayeredConfigTree) -> None:
        for _, sink, edge_attrs in graph.out_edges(self.name, data=True):
            graph.add_edge(
                "input_data",
                sink,
                input_slot=edge_attrs["input_slot"],
                output_slot=edge_attrs["output_slot"],
            )

        for source, _, edge_attrs in graph.in_edges(self.name, data=True):
            graph.add_edge(
                source,
                "input_data",
                input_slot=edge_attrs["input_slot"],
                output_slot=edge_attrs["output_slot"],
            )


class ResultStep(Step):
    """Basic Step for result node."""

    def get_implementation_graph(
        self, graph, pipeline_config: LayeredConfigTree
    ) -> MultiDiGraph:
        graph.add_node("results")

    def update_edges(self, graph, pipeline_config: LayeredConfigTree) -> None:
        for _, sink, edge_attrs in graph.out_edges(self.name, data=True):
            graph.add_edge(
                "results",
                sink,
                input_slot=edge_attrs["input_slot"],
                output_slot=edge_attrs["output_slot"],
            )

        for source, _, edge_attrs in graph.in_edges(self.name, data=True):
            graph.add_edge(
                source,
                "results",
                input_slot=edge_attrs["input_slot"],
                output_slot=edge_attrs["output_slot"],
            )


class ImplementedStep(Step):
    """Step for leaf node tied to a specific single implementation"""

    def get_implementation_graph(
        self, graph, pipeline_config: LayeredConfigTree
    ) -> Implementation:
        """Return a single node with an implementation attribute."""
        implementation_name = pipeline_config[self.name]["implementation"]["name"]
        implementation_config = pipeline_config[self.name]["implementation"]["configuration"]
        implementation = Implementation(
            name=implementation_name,
            step_name=self.name,
            environment_variables=implementation_config.to_dict(),
        )
        graph.add_node(
            implementation_name,
            implementation=implementation,
            out_dir=Path("intermediate") / implementation_name,
        )

    def update_edges(self, graph, pipeline_config: LayeredConfigTree) -> None:
        implementation_name = pipeline_config[self.name]["implementation"]["name"]
        for _, sink, edge_attrs in graph.out_edges(self.name, data=True):
            graph.add_edge(
                implementation_name,
                sink,
                input_slot=edge_attrs["input_slot"],
                output_slot=edge_attrs["output_slot"],
            )

        for source, _, edge_attrs in graph.in_edges(self.name, data=True):
            graph.add_edge(
                source,
                implementation_name,
                input_slot=edge_attrs["input_slot"],
                output_slot=edge_attrs["output_slot"],
            )
