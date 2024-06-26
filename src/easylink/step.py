import copy
from abc import ABC, abstractmethod
from typing import Dict, List

import networkx as nx
from layered_config_tree import LayeredConfigTree

from easylink.graph_components import Edge, InputSlot, OutputSlot, SlotMapping
from easylink.implementation import Implementation
from easylink.utilities import paths
from easylink.utilities.data_utils import load_yaml


class Step(ABC):
    """Steps contain information about the purpose of the interoperable elements of
    the sequence called a *Pipeline* and how those elements relate to one another.
    In turn, steps are implemented by Implementations, such that each step may have
    several implementations but each implementation must have exactly one step.
    In a sense, steps contain metadata about the implementations to which they relate.
    """

    def __init__(
        self,
        step_name: str,
        name: str = None,
        input_slots: List[InputSlot] = [],
        output_slots: List[OutputSlot] = [],
    ) -> None:
        self.name = name if name else step_name
        self.step_name = step_name
        self.input_slots = {slot.name: slot for slot in input_slots}
        self.output_slots = {slot.name: slot for slot in output_slots}

    @abstractmethod
    def update_implementation_graph(
        self, graph: nx.MultiDiGraph, step_config: LayeredConfigTree
    ) -> None:
        """Resolve the graph composed of Steps into a graph composed of Implementations."""
        pass

    @abstractmethod
    def validate_step(self, step_config: LayeredConfigTree) -> Dict[str, List[str]]:
        """Validate the step against the pipeline configuration."""
        pass


class IOStep(Step):
    """"""

    @property
    def pipeline_graph_node_name(self):
        return "pipeline_graph_" + self.name

    def update_implementation_graph(
        self, graph: nx.MultiDiGraph, step_config: LayeredConfigTree
    ) -> None:
        """Add a single node to the graph based on step name."""
        graph.add_node(self.pipeline_graph_node_name)
        self.update_edges(graph, step_config)

    def update_edges(self, graph: nx.MultiDiGraph, step_config: LayeredConfigTree) -> None:
        """Add edges to/from self to replace the edges from the current step"""
        for _, sink, edge_attrs in graph.out_edges(self.name, data=True):
            graph.add_edge(
                self.pipeline_graph_node_name,
                sink,
                input_slot=edge_attrs["input_slot"],
                output_slot=edge_attrs["output_slot"],
            )

        for source, _, edge_attrs in graph.in_edges(self.name, data=True):
            graph.add_edge(
                source,
                self.pipeline_graph_node_name,
                input_slot=edge_attrs["input_slot"],
                output_slot=edge_attrs["output_slot"],
            )

    def validate_step(self, step_config: LayeredConfigTree) -> Dict[str, List[str]]:
        return {}


class BasicStep(Step):
    """Step for leaf node tied to a specific single implementation"""

    def update_implementation_graph(
        self, graph: nx.MultiDiGraph, step_config: LayeredConfigTree
    ) -> None:
        """Return a single node with an implementation attribute."""
        implementation_name = step_config[self.name]["implementation"]["name"]
        implementation_config = step_config[self.name]["implementation"]["configuration"]
        implementation_node_name = (
            implementation_name
            if self.name == self.step_name
            else f"{self.name}_{implementation_name}"
        )
        implementation = Implementation(
            name=implementation_name,
            step_name=self.step_name,
            environment_variables=implementation_config.to_dict(),
        )
        graph.add_node(
            implementation_node_name,
            implementation=implementation,
        )
        self.update_edges(graph, step_config)

    def update_edges(self, graph: nx.MultiDiGraph, step_config: LayeredConfigTree) -> None:
        """Add edges to/from the implementation node to replace the edges from the current step"""
        implementation_name = step_config[self.name]["implementation"]["name"]
        implementation_node_name = (
            implementation_name
            if self.name == self.step_name
            else f"{self.name}_{implementation_name}"
        )
        for _source, sink, edge_attrs in graph.out_edges(self.name, data=True):
            graph.add_edge(
                implementation_node_name,
                sink,
                input_slot=edge_attrs["input_slot"],
                output_slot=edge_attrs["output_slot"],
            )

        for source, _sink, edge_attrs in graph.in_edges(self.name, data=True):
            graph.add_edge(
                source,
                implementation_node_name,
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
        step_name: str,
        name: str = None,
        input_slots: List[InputSlot] = [],
        output_slots: List[OutputSlot] = [],
        nodes: List[Step] = [],
        edges: List[Edge] = [],
        slot_mappings: Dict[str, List[SlotMapping]] = {"input": [], "output": []},
    ) -> None:
        super().__init__(step_name, name, input_slots, output_slots)
        self.nodes = nodes
        self.edges = edges
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
                output_slot=graph.nodes[edge.in_node]["step"].output_slots[edge.output_slot],
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
            graph.remove_node(node)

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
        for input_mapping in self.slot_mappings["input"]:
            parent_slot = input_mapping.parent_slot
            child_node = input_mapping.child_node
            child_slot = input_mapping.child_slot
            parent_edge = [
                (_source, _sink, edge_attrs)
                for (_source, _sink, edge_attrs) in graph.in_edges(self.name, data=True)
                if edge_attrs.get("input_slot").name == parent_slot
            ]
            if not parent_edge:
                raise ValueError(f"Edge not found for {self.name} input slot {parent_slot}")
            if len(parent_edge) > 1:
                raise ValueError(
                    f"Multiple edges found for {self.name} input slot {parent_slot}"
                )
            source, _sink, edge_attrs = parent_edge[0]
            graph.add_edge(
                source,
                child_node,
                input_slot=graph.nodes[child_node]["step"].input_slots[child_slot],
                output_slot=edge_attrs["output_slot"],
            )

        for output_mapping in self.slot_mappings["output"]:
            parent_slot = output_mapping.parent_slot
            child_node = output_mapping.child_node
            child_slot = output_mapping.child_slot
            parent_edges = [
                (_source, _sink, edge_attrs)
                for (_source, _sink, edge_attrs) in graph.out_edges(self.name, data=True)
                if edge_attrs.get("output_slot").name == parent_slot
            ]
            for _source, sink, edge_attrs in parent_edges:
                graph.add_edge(
                    child_node,
                    sink,
                    input_slot=edge_attrs["input_slot"],
                    output_slot=graph.nodes[child_node]["step"].output_slots[child_slot],
                )


class HierarchicalStep(CompositeStep, BasicStep):
    """A HierarchicalStep can be a single implementation or several 'substeps'. This requires
    a "substeps" key in the step configuration. If no substeps key is present, it will be treated as
    a single implemented step."""

    @property
    def config_key(self):
        return "substeps"

    def update_implementation_graph(
        self, graph: nx.MultiDiGraph, step_config: LayeredConfigTree
    ) -> None:
        if not self.name in step_config or not self.config_key in step_config[self.name]:
            BasicStep.update_implementation_graph(self, graph, step_config)
        else:
            sub_config = step_config[self.name][self.config_key]
            CompositeStep.update_implementation_graph(self, graph, sub_config)

    def validate_step(self, step_config: LayeredConfigTree) -> Dict[str, List[str]]:
        if not self.name in step_config or not self.config_key in step_config[self.name]:
            return BasicStep.validate_step(self, step_config)
        sub_config = step_config[self.name][self.config_key]
        return CompositeStep.validate_step(self, sub_config)


class LoopStep(CompositeStep, BasicStep):
    """A LoopStep allows a user to loop a step or sequence of steps a user-configured number of times."""

    @property
    def config_key(self):
        return "iterate"

    def update_implementation_graph(
        self, graph: nx.MultiDiGraph, step_config: LayeredConfigTree
    ) -> None:
        if not self.name in step_config or not self.config_key in step_config[self.name]:
            BasicStep.update_implementation_graph(self, graph, step_config)
        else:
            sub_config = step_config[self.name][self.config_key]
            self.graph = self.create_looped_graph(step_config)
            self.slot_mappings = self.get_loop_slot_mappings(step_config)
            loop_config = self.get_loop_config(sub_config)
            CompositeStep.update_implementation_graph(self, graph, loop_config)

            pass

    def validate_step(self, step_config: LayeredConfigTree) -> Dict[str, List[str]]:
        if not self.name in step_config or not self.config_key in step_config[self.name]:
            return BasicStep.validate_step(self, step_config)

        sub_config = step_config[self.name][self.config_key]
        if len(sub_config) == 0:
            return {f"step {self.name}": ["No loops configured under iterate key."]}

        errors = {}
        for i, loop in enumerate(sub_config):
            if len(self.graph.nodes) == 1:
                loop_config = {self.name: loop}
            loop_errors = CompositeStep.validate_step(self, loop_config)
            if loop_errors:
                errors[f"step {self.name}"][f"loop {i+1}"] = loop_errors
        return errors

    def create_looped_graph(self, step_config: LayeredConfigTree):
        sub_config = step_config[self.name][self.config_key]
        num_loops = len(sub_config)
        graph = nx.MultiDiGraph()

        for i in range(num_loops):
            new_loop = nx.MultiDiGraph()
            for node in self.graph.nodes():
                step = self.graph.nodes[node]["step"]
                updated_step = copy.deepcopy(step)
                updated_step.name = f"{node}_loop_{i+1}"
                new_loop.add_node(node, step=updated_step)
            node_names = {node: f"{node}_loop_{i+1}" for node in new_loop.nodes()}
            renamed_loop = nx.relabel_nodes(new_loop, node_names)
            graph.update(renamed_loop)

            if i > 0:
                for edge in self.graph.edges(data=True):
                    source, sink, edge_attrs = edge
                    input_slot = edge_attrs["input_slot"]
                    output_slot = edge_attrs["output_slot"]
                    graph.add_edge(
                        f"{source}_loop_{i}",
                        f"{sink}_loop_{i+1}",
                        input_slot=input_slot,
                        output_slot=output_slot,
                    )
        return graph

    def get_loop_slot_mappings(self, step_config):
        sub_config = step_config[self.name][self.config_key]
        num_loops = len(sub_config)
        input_mappings = [
            SlotMapping("input", self.name, slot, f"{self.name}_loop_1", slot)
            for slot in self.input_slots
        ]
        output_mappings = [
            SlotMapping("output", self.name, slot, f"{self.name}_loop_{num_loops}", slot)
            for slot in self.output_slots
        ]
        return {"input": input_mappings, "output": output_mappings}

    def get_loop_config(self, iterate_config: LayeredConfigTree):
        loop_config = {}
        for i, loop in enumerate(iterate_config):
            loop_config[f"{self.name}_loop_{i+1}"] = loop
        return loop_config
