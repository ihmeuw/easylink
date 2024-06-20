from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Tuple

import networkx as nx
from layered_config_tree import LayeredConfigTree

from easylink.implementation import Implementation
from easylink.utilities import paths
from easylink.utilities.data_utils import load_yaml

if TYPE_CHECKING:
    from easylink.configuration import Config


@dataclass
class InputSlot:
    """InputSlot is a dataclass that represents a single input slot for a step."""

    name: str
    env_var: Optional[str]
    validator: Callable


@dataclass
class Edge:
    """Edge is a dataclass that represents an edge between two nodes in a graph."""

    in_node: str
    out_node: str
    output_slot: str
    input_slot: str


class Step(ABC):
    """Steps contain information about the purpose of the interoperable elements of
    the sequence called a *Pipeline* and how those elements relate to one another.
    In turn, steps are implemented by Implementations, such that each step may have
    several implementations but each implementation must have exactly one step.
    In a sense, steps contain metadata about the implementations to which they relate.
    """

    def __init__(
        self, name: str, input_slots: List[InputSlot] = [], output_slots: List[str] = []
    ) -> None:
        self.name = name
        self.input_slots = {slot.name: slot for slot in input_slots}
        self.output_slots = output_slots

    @abstractmethod
    def update_implementation_graph(
        self, graph: nx.MultiDiGraph, step_config: LayeredConfigTree
    ) -> None:
        """Resolve the graph composted of Steps into a graph composed of Implementations."""
        pass

    @abstractmethod
    def update_edges(self, graph: nx.MultiDiGraph, step_config: LayeredConfigTree) -> None:
        """Update the edges of the graph to replace Step graph edges with Implementation graph edges."""
        pass

    @abstractmethod
    def validate_step(self, step_config: LayeredConfigTree) -> Dict[str, List[str]]:
        """Validate the step against the pipeline configuration."""
        pass


class InputStep(Step):
    """Basic Step for input data node."""

    def update_implementation_graph(
        self, graph: nx.MultiDiGraph, step_config: LayeredConfigTree
    ) -> None:
        """Add a single node to the graph with the name 'input_data'."""
        graph.add_node("input_data")

    def update_edges(self, graph: nx.MultiDiGraph, step_config: LayeredConfigTree) -> None:
        """Add edges to/from "input_data" to replace the edges from the current step"""
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

    def validate_step(self, step_config: LayeredConfigTree) -> Dict[str, List[str]]:
        return {}


class ResultStep(Step):
    """Basic Step for result node."""

    def update_implementation_graph(
        self, graph: nx.MultiDiGraph, step_config: LayeredConfigTree
    ) -> None:
        """Add a single node to the graph with the name 'results'."""
        graph.add_node("results")

    def update_edges(self, graph: nx.MultiDiGraph, step_config: LayeredConfigTree) -> None:
        """Add edges to/from "results" to replace the edges from the current step"""
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

    def validate_step(self, step_config: LayeredConfigTree) -> Dict[str, List[str]]:
        return {}


class ImplementedStep(Step):
    """Step for leaf node tied to a specific single implementation"""

    def update_implementation_graph(
        self, graph: nx.MultiDiGraph, step_config: LayeredConfigTree
    ) -> None:
        """Return a single node with an implementation attribute."""
        implementation_name = step_config[self.name]["implementation"]["name"]
        implementation_config = step_config[self.name]["implementation"]["configuration"]
        implementation = Implementation(
            name=implementation_name,
            step_name=self.name,
            environment_variables=implementation_config.to_dict(),
        )
        graph.add_node(
            implementation_name,
            implementation=implementation,
        )

    def update_edges(self, graph: nx.MultiDiGraph, step_config: LayeredConfigTree) -> None:
        """Add edges to/from the implementation node to replace the edges from the current step"""
        implementation_name = step_config[self.name]["implementation"]["name"]
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

    def validate_step(self, step_config: LayeredConfigTree) -> Dict[str, List[str]]:
        """Return error strings if the step configuration is incorrect."""
        errors = {}
        metadata = load_yaml(paths.IMPLEMENTATION_METADATA)
        if not self.name in step_config:
            errors[f"step {self.name}"] = ["The step is not configured."]
        elif not "implementation" in step_config[self.name]:
            errors[f"step {self.name}"] = [
                "The step configuration does not contain an 'implementation' key."
            ]
        elif not "name" in step_config[self.name]["implementation"]:
            errors[f"step {self.name}"] = [
                "The implementation configuration does not contain a 'name' key."
            ]
        elif not step_config[self.name]["implementation"]["name"] in metadata:
            errors[f"step {self.name}"] = [
                f"Implementation '{step_config[self.name]['implementation']['name']}' is not supported. "
                f"Supported implementations are: {list(metadata.keys())}."
            ]
        return errors


class CompositeStep(Step):
    """Composite Steps are Steps that contain other Steps. They allow operations to be
    applied recursively on Steps or sequences of Steps."""

    def __init__(
        self,
        name: str,
        input_slots: List[InputSlot] = [],
        output_slots: List[str] = [],
        nodes: List[Step] = [],
        edges: List[Edge] = [],
        slot_mappings: Dict[str, List[Tuple[str]]] = {"input": [], "output": []},
    ) -> None:
        super().__init__(name, input_slots, output_slots)
        self.graph = self._create_graph(nodes, edges)
        self.slot_mappings = slot_mappings

    def _create_graph(self, nodes: List[Step], edges: List[Edge]) -> nx.MultiDiGraph:
        """Create a MultiDiGraph from the nodes and edges the step was initialized with."""
        graph = nx.MultiDiGraph()
        for step in nodes:
            graph.add_node(
                step.name,
                step=step,
            )
        for edge in edges:
            graph.add_edge(
                edge.in_node,
                edge.out_node,
                input_slot=graph.nodes[edge.out_node]["step"].input_slots[edge.input_slot],
                output_slot=edge.output_slot,
            )

        return graph

    def update_implementation_graph(
        self, graph: nx.MultiDiGraph, step_config: LayeredConfigTree
    ) -> None:
        """Call update_implementation_graph on each subgraph node and update the graph."""
        graph.update(self.graph)
        self.remap_slots(graph, step_config)
        for node in self.graph.nodes:
            step = self.graph.nodes[node]["step"]
            step.update_implementation_graph(graph, step_config)
            step.update_edges(graph, step_config)
            graph.remove_node(node)

    def update_edges(self, graph, step_config: LayeredConfigTree) -> None:
        pass

    def validate_step(self, step_config: LayeredConfigTree) -> Dict[str, List[str]]:
        """Validate each step in the subgraph in turn. Also return errors for any extra steps."""
        errors = {}
        for node in self.graph.nodes:
            step = self.graph.nodes[node]["step"]
            step_errors = step.validate_step(step_config)
            if step_errors:
                errors.update(step_errors)
        extra_steps = set(step_config.keys()) - set(self.graph.nodes)
        for extra_step in extra_steps:
            errors[f"step {extra_step}"] = [f"{extra_step} is not a valid step."]
        return errors

    def remap_slots(self, graph: nx.MultiDiGraph, step_config: LayeredConfigTree) -> None:
        """Update edges to reflect mapping between parent and child nodes for input and output slots."""
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


class HierarchicalStep(CompositeStep, ImplementedStep):
    """A HierarchicalStep can be a single implementation or several 'substeps'. This requires
    a "substeps" key in the step configuration. If no substeps key is present, it will be treated as
    a single implemented step."""

    @property
    def config_key(self):
        return "substeps"

    def update_implementation_graph(
        self, graph: nx.MultiDiGraph, step_config: LayeredConfigTree
    ) -> None:
        sub_config = step_config[self.name]
        if not self.config_key in sub_config:
            ImplementedStep.update_implementation_graph(self, graph, step_config)
            ImplementedStep.update_edges(self, graph, step_config)
        else:
            sub_config = sub_config[self.config_key]
            CompositeStep.update_implementation_graph(self, graph, sub_config)

    def validate_step(self, step_config: LayeredConfigTree) -> Dict[str, List[str]]:
        if not self.name in step_config or not self.config_key in step_config:
            return ImplementedStep.validate_step(self, step_config)
        else:
            sub_config = sub_config[self.name][self.config_key]
            return CompositeStep.validate_step(self, sub_config)
