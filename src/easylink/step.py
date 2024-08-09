import copy
from abc import ABC, abstractmethod
from collections import defaultdict
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
        self.parent_step = None

    def set_parent_step(self, step):
        self.parent_step = step

    @abstractmethod
    def update_implementation_graph(
        self, graph: nx.MultiDiGraph, step_config: LayeredConfigTree
    ) -> None:
        """Resolve the graph composed of Steps into a graph composed of Implementations."""
        pass

    @abstractmethod
    def validate_step(
        self, step_config: LayeredConfigTree, input_data_config: LayeredConfigTree
    ) -> Dict[str, List[str]]:
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
        graph.remove_node(self.name)

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

    def validate_step(
        self, step_config: LayeredConfigTree, input_data_config: LayeredConfigTree
    ) -> Dict[str, List[str]]:
        return {}


class BasicStep(Step):
    """Step for leaf node tied to a specific single implementation"""

    def update_implementation_graph(
        self, graph: nx.MultiDiGraph, step_config: LayeredConfigTree
    ) -> None:
        """Return a single node with an implementation attribute."""
        implementation_config = step_config["implementation"]
        implementation_node_name = self.get_implementation_node_name(step_config)
        implementation = Implementation(
            step_name=self.step_name,
            implementation_config=implementation_config,
        )
        graph.add_node(
            implementation_node_name,
            implementation=implementation,
        )
        self.update_edges(graph, step_config)
        graph.remove_node(self.name)

    def update_edges(self, graph: nx.MultiDiGraph, step_config: LayeredConfigTree) -> None:
        """Add edges to/from the implementation node to replace the edges from the current step"""
        implementation_node_name = self.get_implementation_node_name(step_config)
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

    def validate_step(
        self, step_config: LayeredConfigTree, input_data_config: LayeredConfigTree
    ) -> Dict[str, List[str]]:
        """Return error strings if the step configuration is incorrect."""
        errors = {}
        metadata = load_yaml(paths.IMPLEMENTATION_METADATA)
        if not "implementation" in step_config:
            errors[f"step {self.name}"] = [
                "The step configuration does not contain an 'implementation' key."
            ]
        elif not "name" in step_config["implementation"]:
            errors[f"step {self.name}"] = [
                "The implementation configuration does not contain a 'name' key."
            ]
        elif not step_config["implementation"]["name"] in metadata:
            errors[f"step {self.name}"] = [
                f"Implementation '{step_config['implementation']['name']}' is not supported. "
                f"Supported implementations are: {list(metadata.keys())}."
            ]
        return errors

    def get_implementation_node_name(self, step_config: LayeredConfigTree) -> str:
        """Resolve a sensible unique node name for the implementation graph.
        This method compares the step node names with the step names through the step hierarchy and
        uses the full suffix of step names starting from wherever the two first differ. For example,
        loop steps may have multiple loops with the same implementation and step, e.g. "step_3" and "step_3_python_pandas".
        If we have node names step_3_loop_1 step_3_python_pandas the resulting implementation node name
        will be step_3_loop_1_step_3_python_pandas. If all the node names and step names match, we have not
        introduced any step degeneracies with e.g. loops or multiples, and we can simply use the implementation
        name."""
        step = self
        node_names = []
        step_names = []
        while step:
            node_names.append(step.name)
            step_names.append(step.step_name)
            step = step.parent_step

        implementation_names = []
        step_names.reverse()
        node_names.reverse()
        for i, (step_name, node_name) in enumerate(zip(step_names, node_names)):
            if step_name != node_name:
                implementation_names = node_names[i:]
                break
        implementation_names.append(step_config["implementation"]["name"])
        return "_".join(implementation_names)


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
        for node in self.nodes:
            node.set_parent_step(self)
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
                edge.source_node,
                edge.target_node,
                input_slot=graph.nodes[edge.target_node]["step"].input_slots[edge.input_slot],
                output_slot=graph.nodes[edge.source_node]["step"].output_slots[
                    edge.output_slot
                ],
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
            sub_config = step_config if isinstance(step, IOStep) else step_config[step.name]
            step.update_implementation_graph(graph, sub_config)
        graph.remove_node(self.name)

    def validate_step(
        self, step_config: LayeredConfigTree, input_data_config: LayeredConfigTree
    ) -> Dict[str, List[str]]:
        """Validate each step in the subgraph in turn. Also return errors for any extra steps."""
        errors = {}
        for node in self.graph.nodes:
            step = self.graph.nodes[node]["step"]
            if isinstance(step, IOStep):
                continue
            if step.name not in step_config:
                step_errors = {f"step {step.name}": [f"The step is not configured."]}
            else:
                step_errors = step.validate_step(step_config[step.name], input_data_config)
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
        if not self.config_key in step_config:
            BasicStep.update_implementation_graph(self, graph, step_config)
        else:
            CompositeStep.update_implementation_graph(
                self, graph, step_config[self.config_key]
            )

    def validate_step(
        self, step_config: LayeredConfigTree, input_data_config: LayeredConfigTree
    ) -> Dict[str, List[str]]:
        if not self.config_key in step_config:
            return BasicStep.validate_step(self, step_config, input_data_config)
        sub_config = step_config[self.config_key]
        return CompositeStep.validate_step(self, sub_config, input_data_config)


class LoopStep(CompositeStep, BasicStep):
    """A LoopStep allows a user to loop a single step or a sequence of steps a user-configured number of times."""

    def __init__(
        self,
        step_name: str,
        name: str = None,
        input_slots: List[InputSlot] = [],
        output_slots: List[OutputSlot] = [],
        iterated_node: Step = None,
        self_edges: List[Edge] = [],
    ) -> None:
        super(CompositeStep, self).__init__(step_name, name, input_slots, output_slots)
        if not iterated_node or iterated_node.name != step_name:
            raise NotImplementedError(
                f"LoopStep {self.name} must be initialized with a single node with the same name."
            )
        self.iterated_node = iterated_node
        self.iterated_node.set_parent_step(self)
        for edge in self_edges:
            if not edge.source_node == edge.target_node == step_name:
                raise NotImplementedError(
                    f"LoopStep {self.name} must be initialized with only self-loops as edges"
                )
        self.self_edges = self_edges

    @property
    def config_key(self):
        return "iterate"

    def update_implementation_graph(
        self, graph: nx.MultiDiGraph, step_config: LayeredConfigTree
    ) -> None:
        if not self.config_key in step_config:
            BasicStep.update_implementation_graph(self, graph, step_config)
        else:
            sub_config = step_config[self.config_key]
            num_loops = len(sub_config)
            self.graph = self._create_looped_graph(num_loops)
            self.slot_mappings = self._get_loop_slot_mappings(num_loops)
            loop_config = self._get_loop_config(sub_config)
            CompositeStep.update_implementation_graph(self, graph, loop_config)

    def validate_step(
        self, step_config: LayeredConfigTree, input_data_config: LayeredConfigTree
    ) -> Dict[str, List[str]]:
        if not self.config_key in step_config:
            return BasicStep.validate_step(self, step_config, input_data_config)

        sub_config = step_config[self.config_key]

        if not isinstance(sub_config, list):
            return {
                f"step {self.name}": [
                    "Loops must be formatted as a sequence in the pipeline configuration."
                ]
            }

        if len(sub_config) == 0:
            return {f"step {self.name}": ["No loops configured under iterate key."]}

        errors = defaultdict(dict)
        for i, loop in enumerate(sub_config):
            loop_errors = self.iterated_node.validate_step(loop, input_data_config)
            if loop_errors:
                errors[f"step {self.name}"][f"loop {i+1}"] = loop_errors
        return errors

    def _create_looped_graph(self, num_loops: int) -> nx.MultiDiGraph:
        """Make N copies of the iterated graph and chain them together according
        to the self edges."""
        graph = nx.MultiDiGraph()

        for i in range(num_loops):
            updated_step = copy.deepcopy(self.iterated_node)
            updated_step.name = f"{self.name}_loop_{i+1}"
            graph.add_node(updated_step.name, step=updated_step)
            if i > 0:
                for edge in self.self_edges:
                    source = f"{self.name}_loop_{i}"
                    sink = f"{self.name}_loop_{i+1}"
                    input_slot = graph.nodes[sink]["step"].input_slots[edge.input_slot]
                    output_slot = graph.nodes[source]["step"].output_slots[edge.output_slot]
                    graph.add_edge(
                        source,
                        sink,
                        input_slot=input_slot,
                        output_slot=output_slot,
                    )
        return graph

    def _get_loop_slot_mappings(self, num_loops: int) -> nx.MultiDiGraph:
        """Get the appropriate slot mappings based on the number of loops
        and the non-self-edge input and output slots."""
        input_mappings = []
        self_edge_input_slots = {edge.input_slot for edge in self.self_edges}
        external_input_slots = self.input_slots.keys() - self_edge_input_slots
        for input_slot in self_edge_input_slots:
            input_mappings.append(
                SlotMapping("input", self.name, input_slot, f"{self.name}_loop_1", input_slot)
            )
        for input_slot in external_input_slots:
            input_mappings.extend(
                [
                    SlotMapping(
                        "input", self.name, input_slot, f"{self.name}_loop_{n+1}", input_slot
                    )
                    for n in range(num_loops)
                ]
            )
        output_mappings = [
            SlotMapping("output", self.name, slot, f"{self.name}_loop_{num_loops}", slot)
            for slot in self.output_slots
        ]
        return {"input": input_mappings, "output": output_mappings}

    def _get_loop_config(
        self, iterate_config: LayeredConfigTree
    ) -> Dict[str, LayeredConfigTree]:
        """Get the dictionary for the looped graph based on the sequence
        of sub-yamls."""
        loop_config = {}
        for i, loop in enumerate(iterate_config):
            loop_config[f"{self.name}_loop_{i+1}"] = loop
        return LayeredConfigTree(loop_config)


class ParallelStep(CompositeStep, BasicStep):
    """A ParallelStep allows a user to run a sequence of steps in parallel."""

    def __init__(
        self,
        step_name: str,
        name: str = None,
        input_slots: List[InputSlot] = [],
        output_slots: List[OutputSlot] = [],
        split_node: Step = None,
    ) -> None:
        super(CompositeStep, self).__init__(step_name, name, input_slots, output_slots)
        if not split_node or split_node.name != step_name:
            raise NotImplementedError(
                f"ParallelStep {self.name} must be initialized with a single node with the same name."
            )
        self.split_node = split_node
        self.split_node.set_parent_step(self)

    @property
    def config_key(self):
        return "parallel"

    def update_implementation_graph(
        self, graph: nx.MultiDiGraph, step_config: LayeredConfigTree
    ) -> None:
        if not self.config_key in step_config:
            BasicStep.update_implementation_graph(self, graph, step_config)
        else:
            sub_config = step_config[self.config_key]
            num_splits = len(sub_config)
            self.graph = self._create_parallel_graph(num_splits)
            self.slot_mappings = self._get_parallel_slot_mappings(num_splits)
            parallel_config = self._get_parallel_config(sub_config)
            CompositeStep.update_implementation_graph(self, graph, parallel_config)

    def validate_step(
        self, step_config: LayeredConfigTree, input_data_config: LayeredConfigTree
    ) -> Dict[str, List[str]]:
        if not self.config_key in step_config:
            return BasicStep.validate_step(self, step_config, input_data_config)

        sub_config = step_config[self.config_key]

        if not isinstance(sub_config, list):
            return {
                f"step {self.name}": [
                    "Loops must be formatted as a sequence in the pipeline configuration."
                ]
            }

        if len(sub_config) == 0:
            return {f"step {self.name}": ["No loops configured under iterate key."]}

        errors = defaultdict(dict)
        for i, parallel_config in enumerate(sub_config):
            parallel_errors = {}
            input_data_file = parallel_config.get("input_data_file")
            if input_data_file and not input_data_file in input_data_config:
                parallel_errors["Input Data Key"] = [
                    f"Input data file '{input_data_file}' not found in input data configuration."
                ]
            parallel_errors.update(
                self.split_node.validate_step(parallel_config, input_data_config)
            )
            if parallel_errors:
                errors[f"step {self.name}"][f"parallel_split_{i+1}"] = parallel_errors
        return errors

    def _create_parallel_graph(self, num_splits: int) -> nx.MultiDiGraph:
        """Make N copies of the iterated graph and chain them together according
        to the self edges."""
        graph = nx.MultiDiGraph()

        for i in range(num_splits):
            updated_step = copy.deepcopy(self.split_node)
            updated_step.name = f"{self.name}_parallel_split_{i+1}"
            graph.add_node(updated_step.name, step=updated_step)
        return graph

    def _get_parallel_slot_mappings(self, num_splits) -> nx.MultiDiGraph:
        """Get the appropriate slot mappings based on the number of loops
        and the non-self-edge input and output slots."""
        input_mappings = [
            SlotMapping("input", self.name, slot, f"{self.name}_parallel_split_{n+1}", slot)
            for n in range(num_splits)
            for slot in self.input_slots
        ]
        output_mappings = [
            SlotMapping("output", self.name, slot, f"{self.name}_parallel_split_{n+1}", slot)
            for n in range(num_splits)
            for slot in self.output_slots
        ]
        return {"input": input_mappings, "output": output_mappings}

    def _get_parallel_config(
        self, step_config: LayeredConfigTree
    ) -> Dict[str, LayeredConfigTree]:
        """Get the dictionary for the parallel graph based on the sequence
        of sub-yamls."""
        parallel_step_config = {}
        for i, config in enumerate(step_config):
            parallel_step_config[f"{self.name}_parallel_split_{i+1}"] = config
        return LayeredConfigTree(parallel_step_config)

    def remap_slots(self, graph: nx.MultiDiGraph, step_config: LayeredConfigTree) -> None:
        super().remap_slots(graph, step_config)
        for step_name, config in step_config.items():
            if "input_data_file" in config:
                for edge_idx in graph["pipeline_graph_input_data"][step_name]:
                    graph["pipeline_graph_input_data"][step_name][edge_idx][
                        "output_slot"
                    ] = OutputSlot(name=config["input_data_file"])
