import copy
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Dict, List
import itertools

import networkx as nx
from layered_config_tree import LayeredConfigTree

from easylink.graph_components import (
    StepGraphEdge,
    ImplementationGraphEdge,
    InputSlot,
    OutputSlot,
    SlotMapping,
    StepSlotMapping,
    ImplementationSlotMapping,
    StepGraph,
    ImplementationGraph,
)
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
        self._config = None
    
    def __hash__(self) -> int:
        return hash(self.name)
    
    

    @property
    def config(self):
        if self._config is None:
            raise ValueError(f"Step {self.name}'s config was invoked before being set")
        return self._config

    @abstractmethod
    def set_step_config(self, parent_config: LayeredConfigTree):
        pass

    def set_parent_step(self, step):
        self.parent_step = step

    @abstractmethod
    def get_implementation_graph(self) -> None:
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
    def implementation_graph_node_name(self):
        return "pipeline_graph_" + self.name

    def set_step_config(self, parent_config: LayeredConfigTree):
        self._config = parent_config

    def get_implementation_graph(self) -> ImplementationGraph:
        """Add a single node to the graph based on step name."""
        implementation_graph = ImplementationGraph()
        implementation_graph.add_node_from_impl(self.implementation_graph_node_name, implementation=None)
        return implementation_graph
    
    def get_implementation_edges(self, edge: StepGraphEdge):
        implementation_edges = []
        if edge.source_node == self.name:
            mappings = [mapping for mapping in self.implementation_slot_mappings()["output"] if mapping.slot == edge.output_slot]
            for mapping in mappings:
                imp_edge = mapping.propagate_edge(self, edge)
                implementation_edges.append(imp_edge)
            
        elif edge.target_node == self.name:
            mappings = [mapping for mapping in self.implementation_slot_mappings()["input"] if mapping.slot == edge.input_slot]
            for mapping in mappings:
                imp_edge = mapping.propagate_edge(self, edge)
                implementation_edges.append(imp_edge)
        else:
            raise ValueError(f"IOStep {self.name} not in edge {edge}")
        if not implementation_edges:
            raise ValueError(f"No edges found for IOStep {self.name} in edge {edge}")
        return implementation_edges
        

    def implementation_slot_mappings(self) -> Dict[str, List[SlotMapping]]:
        return {
            "input": [
                ImplementationSlotMapping(
                    "input", self.name, slot, self.implementation_graph_node_name)
                for slot in self.input_slots
            ],
            "output": [
                ImplementationSlotMapping(
                    "output", self.name, slot, self.implementation_graph_node_name)
                for slot in self.output_slots
            ],
        }


    def validate_step(
        self, step_config: LayeredConfigTree, input_data_config: LayeredConfigTree
    ) -> Dict[str, List[str]]:
        return {}


class BasicStep(Step):
    """Step for leaf node tied to a specific single implementation"""

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
        self._config = None

    def set_step_config(self, parent_config: LayeredConfigTree) -> None:
        self._config = parent_config[self.name]

    def get_implementation_graph(self) -> ImplementationGraph:
        implementation_graph = ImplementationGraph()
        """Return a single node with an implementation attribute."""
        implementation_config = self.config["implementation"]
        implementation_node_name = self.get_implementation_node_name()
        implementation = Implementation(
            step_name=self.step_name,
            implementation_config=implementation_config,
        )
        implementation_graph.add_node_from_impl(
            implementation_node_name,
            implementation=implementation,
        )
        return implementation_graph
    
    def get_implementation_edges(self, edge: StepGraphEdge):
        implementation_edges = []
        if edge.source_node == self.name:
            mappings = [mapping for mapping in self.implementation_slot_mappings()["output"] if mapping.slot == edge.output_slot]
            for mapping in mappings:
                imp_edge = mapping.propagate_edge(self, edge)
                implementation_edges.append(imp_edge)
            
        elif edge.target_node == self.name:
            mappings = [mapping for mapping in self.implementation_slot_mappings()["input"] if mapping.slot == edge.input_slot]
            for mapping in mappings:
                imp_edge = mapping.propagate_edge(self, edge)
                implementation_edges.append(imp_edge)
        else:
            raise ValueError(f"IOStep {self.name} not in edge {edge}")
        if not implementation_edges:
            raise ValueError(f"No edges found for IOStep {self.name} in edge {edge}")
        return implementation_edges
    
    def implementation_slot_mappings(self) -> Dict[str, List[ImplementationSlotMapping]]:
        return {
            "input": [
                ImplementationSlotMapping(
                    "input", self.name, slot, self.get_implementation_node_name())
                for slot in self.input_slots
            ],
            "output": [
                ImplementationSlotMapping(
                    "output", self.name, slot, self.get_implementation_node_name())
                for slot in self.output_slots
            ],
        }

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

    def get_implementation_node_name(self) -> str:
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
        implementation_names.append(self.config["implementation"]["name"])
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
        edges: List[StepGraphEdge] = [],
        slot_mappings: Dict[str, List[StepSlotMapping]] = {"input": [], "output": []},
    ) -> None:
        super().__init__(step_name, name, input_slots, output_slots)
        self.nodes = nodes
        for node in self.nodes:
            node.set_parent_step(self)
        self.edges = edges
        self.step_graph = self._create_graph(nodes, edges)
        self.step_slot_mappings = slot_mappings

    def set_step_config(self, parent_config: LayeredConfigTree) -> None:
        self._config = parent_config[self.name]

    def _create_graph(self, nodes: List[Step], edges: List[StepGraphEdge]) -> StepGraph:
        """Create a MultiDiGraph from the nodes and edges the step was initialized with."""
        step_graph = StepGraph()
        for step in nodes:
            step_graph.add_node_from_step(step)
        for edge in edges:
            step_graph.add_edge_from_data(edge)
        return step_graph
    
    def update_nodes(self,implementation_graph: ImplementationGraph) -> None:
        for node in self.step_graph.nodes:
            step = self.step_graph.nodes[node]["step"]
            step.set_step_config(self.config)
            implementation_graph.update(step.get_implementation_graph())
    
    def get_implementation_edges(self, edge: StepGraphEdge):
        implementation_edges = []
        if edge.source_node == self.name:
            mappings = [mapping for mapping in self.step_slot_mappings["output"] if mapping.parent_slot == edge.output_slot]
            for mapping in mappings:
                imp_edge = mapping.propagate_edge(edge)
                implementation_edges.append(imp_edge)
            
        elif edge.target_node == self.name:
            mappings = [mapping for mapping in self.step_slot_mappings["input"] if mapping.parent_slot == edge.input_slot]
            for mapping in mappings:
                imp_edge = mapping.propagate_edge(edge)
                implementation_edges.append(imp_edge)
        else:
            raise ValueError(f"IOStep {self.name} not in edge {edge}")
        if not implementation_edges:
            raise ValueError(f"No edges found for IOStep {self.name} in edge {edge}")
        return implementation_edges
    
    def update_edges(self,implementation_graph: ImplementationGraph) -> None:
        for source, target, edge_attrs in self.step_graph.edges(data=True):
            all_edges = []
            edge = StepGraphEdge.from_graph_edge(source, target, edge_attrs)
            parent_source_step = self.step_graph.nodes[source]["step"]
            parent_target_step = self.step_graph.nodes[target]["step"]
            
            source_edges = parent_source_step.get_implementation_edges(edge)
            for source_edge in source_edges:
                for target_edge in parent_target_step.get_implementation_edges(source_edge):
                    all_edges.append(target_edge)
            
            for edge in all_edges:
                implementation_graph.add_edge_from_data(edge)

    def get_implementation_graph(self) -> ImplementationGraph:
        """Call get_implementation_graph on each subgraph node and update the graph."""
        implementation_graph = ImplementationGraph()
        self.update_nodes(implementation_graph)
        self.update_edges(implementation_graph)
        return implementation_graph

    def validate_step(
        self, step_config: LayeredConfigTree, input_data_config: LayeredConfigTree
    ) -> Dict[str, List[str]]:
        """Validate each step in the subgraph in turn. Also return errors for any extra steps."""
        errors = {}
        for node in self.step_graph.nodes:
            step = self.step_graph.nodes[node]["step"]
            if isinstance(step, IOStep):
                continue
            if step.name not in step_config:
                step_errors = {f"step {step.name}": [f"The step is not configured."]}
            else:
                step_errors = step.validate_step(step_config[step.name], input_data_config)
            if step_errors:
                errors.update(step_errors)
        extra_steps = set(step_config.keys()) - set(self.step_graph.nodes)
        for extra_step in extra_steps:
            errors[f"step {extra_step}"] = [f"{extra_step} is not a valid step."]
        return errors


class HierarchicalStep(CompositeStep, BasicStep):
    """A HierarchicalStep can be a single implementation or several 'substeps'. This requires
    a "substeps" key in the step configuration. If no substeps key is present, it will be treated as
    a single implemented step."""

    @property
    def config_key(self):
        return "substeps"

    def set_step_config(self, parent_config: LayeredConfigTree) -> None:
        step_config = parent_config[self.name]
        self._config = (
            step_config
            if not self.config_key in step_config
            else step_config[self.config_key]
        )

    def get_implementation_graph(self) -> ImplementationGraph:
        if len(self.config) > 1:
            return CompositeStep.get_implementation_graph(self)
        return BasicStep.get_implementation_graph(self)
    
    def get_implementation_edges(self, edge: StepGraphEdge):
        if len(self.config) > 1:
            return CompositeStep.get_implementation_edges(self, edge)
        return BasicStep.get_implementation_edges(self, edge)

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
        template_step: Step = None,
        self_edges: List[StepGraphEdge] = [],
    ) -> None:
        super(CompositeStep, self).__init__(step_name, name, input_slots, output_slots)
        if not template_step or template_step.name != step_name:
            raise NotImplementedError(
                f"LoopStep {self.name} must be initialized with a single node with the same name."
            )
        self.template_step = template_step
        self.template_step.set_parent_step(self)
        for edge in self_edges:
            if not edge.source_node == edge.target_node == step_name:
                raise NotImplementedError(
                    f"LoopStep {self.name} must be initialized with only self-loops as edges"
                )
        self.self_edges = self_edges

    @property
    def config_key(self):
        return "iterate"

    def set_step_config(self, parent_config: LayeredConfigTree) -> None:
        step_config = parent_config[self.name]
        if not self.config_key in step_config:
            self._config = step_config
        else:
            self._config = self._get_loop_config(step_config[self.config_key])

    def get_implementation_graph(self) -> None:
        if len(self.config) > 1:
            num_loops = len(self.config)
            self.step_graph = self._create_looped_graph(num_loops)
            self.step_slot_mappings = self._get_loop_slot_mappings(num_loops)
            return CompositeStep.get_implementation_graph(self)
        return BasicStep.get_implementation_graph(self)
    
    def get_implementation_edges(self, edge: StepGraphEdge):
        if len(self.config) > 1:
            return CompositeStep.get_implementation_edges(self, edge)
        return BasicStep.get_implementation_edges(self, edge)

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
            loop_errors = self.template_step.validate_step(loop, input_data_config)
            if loop_errors:
                errors[f"step {self.name}"][f"loop {i+1}"] = loop_errors
        return errors

    def _create_looped_graph(self, num_loops: int) -> StepGraph:
        """Make N copies of the iterated graph and chain them together according
        to the self edges."""
        graph = StepGraph()
        nodes = []
        edges = []

        for i in range(num_loops):
            self.template_step.parent_step = None
            updated_step = copy.deepcopy(self.template_step)
            updated_step.set_parent_step(self)
            updated_step.name = f"{self.name}_loop_{i+1}"
            nodes.append(updated_step)
            if i > 0:
                for self_edge in self.self_edges:
                    source_node=f"{self.name}_loop_{i}"
                    target_node=f"{self.name}_loop_{i+1}"
                    edge = StepGraphEdge(
                    source_node=source_node,
                    target_node=target_node,
                    input_slot=self_edge.input_slot,
                    output_slot=self_edge.output_slot,
                    )
                    edges.append(edge)
                    
        for node in nodes:
            graph.add_node_from_step(node)
        for edge in edges:
            graph.add_edge_from_data(edge)
        return graph

    def _get_loop_slot_mappings(self, num_loops: int) -> dict:
        """Get the appropriate slot mappings based on the number of loops
        and the non-self-edge input and output slots."""
        input_mappings = []
        self_edge_input_slots = {edge.input_slot for edge in self.self_edges}
        external_input_slots = self.input_slots.keys() - self_edge_input_slots
        for input_slot in self_edge_input_slots:
            input_mappings.append(
                StepSlotMapping("input", self.name, input_slot, f"{self.name}_loop_1", input_slot)
            )
        for input_slot in external_input_slots:
            input_mappings.extend(
                [
                    StepSlotMapping(
                        "input", self.name, input_slot, f"{self.name}_loop_{n+1}", input_slot
                    )
                    for n in range(num_loops)
                ]
            )
        output_mappings = [
            StepSlotMapping("output", self.name, slot, f"{self.name}_loop_{num_loops}", slot)
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
        template_step: Step = None,
    ) -> None:
        super(CompositeStep, self).__init__(step_name, name, input_slots, output_slots)
        if not template_step or template_step.name != step_name:
            raise NotImplementedError(
                f"ParallelStep {self.name} must be initialized with a single node with the same name."
            )
        self.template_step = template_step
        self.template_step.set_parent_step(self)

    @property
    def config_key(self):
        return "parallel"

    def set_step_config(self, parent_config: LayeredConfigTree) -> None:
        step_config = parent_config[self.name]
        if not self.config_key in step_config:
            self._config = step_config
        else:
            self._config = self._get_parallel_config(step_config[self.config_key])

    def get_implementation_graph(self) -> None:
        if len(self.config) > 1:
            num_splits = len(self.config)
            self.step_graph = self._create_parallel_graph(num_splits)
            self.step_slot_mappings = self._get_parallel_slot_mappings(num_splits)
            return CompositeStep.get_implementation_graph(self)
        return BasicStep.get_implementation_graph(self)
    
    def get_implementation_edges(self, edge: StepGraphEdge):
        if len(self.config) > 1:
            return CompositeStep.get_implementation_edges(self, edge)
        return BasicStep.get_implementation_edges(self, edge)

    def validate_step(
        self, step_config: LayeredConfigTree, input_data_config: LayeredConfigTree
    ) -> Dict[str, List[str]]:
        if not self.config_key in step_config:
            return BasicStep.validate_step(self, step_config, input_data_config)

        sub_config = step_config[self.config_key]

        if not isinstance(sub_config, list):
            return {
                f"step {self.name}": [
                    "Parallel instances must be formatted as a sequence in the pipeline configuration."
                ]
            }

        if len(sub_config) == 0:
            return {
                f"step {self.name}": [
                    "No parallel instances configured under 'parallel' key."
                ]
            }

        errors = defaultdict(dict)
        for i, parallel_config in enumerate(sub_config):
            parallel_errors = {}
            input_data_file = parallel_config.get("input_data_file")
            if input_data_file and not input_data_file in input_data_config:
                parallel_errors["Input Data Key"] = [
                    f"Input data file '{input_data_file}' not found in input data configuration."
                ]
            parallel_errors.update(
                self.template_step.validate_step(parallel_config, input_data_config)
            )
            if parallel_errors:
                errors[f"step {self.name}"][f"parallel_split_{i+1}"] = parallel_errors
        return errors

    def _create_parallel_graph(self, num_splits: int) -> StepGraph:
        """Make N copies of the template step that are independent and contain the same edges as the
        current step"""
        graph = StepGraph()

        for i in range(num_splits):
            self.template_step.parent_step = None
            updated_step = copy.deepcopy(self.template_step)
            updated_step.set_parent_step(self)
            updated_step.name = f"{self.name}_parallel_split_{i+1}"
            graph.add_node_from_step(updated_step)
        return graph

    def _get_parallel_slot_mappings(self, num_splits) -> nx.MultiDiGraph:
        """Get the appropriate slot mappings based on the number of parallel copies
        and the existing input and output slots."""
        input_mappings = [
            StepSlotMapping("input", self.name, slot, f"{self.name}_parallel_split_{n+1}", slot)
            for n in range(num_splits)
            for slot in self.input_slots
        ]
        output_mappings = [
            StepSlotMapping("output", self.name, slot, f"{self.name}_parallel_split_{n+1}", slot)
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

    # def remap_slots(self, graph: StepGraph, step_config: LayeredConfigTree) -> None:
    #     super().remap_slots(graph, step_config)
    #     for step_name, config in step_config.items():
    #         if "input_data_file" in config:
    #             for edge_idx in graph["pipeline_graph_input_data"][step_name]:
    #                 graph["pipeline_graph_input_data"][step_name][edge_idx]["output_slot"] = (
    #                     OutputSlot(name=config["input_data_file"])
    #                 )
