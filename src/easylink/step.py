import copy
from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Dict, List

import networkx as nx
from layered_config_tree import LayeredConfigTree

from easylink.graph_components import (
    Edge,
    ImplementationGraph,
    InputSlot,
    InputSlotMapping,
    OutputSlot,
    OutputSlotMapping,
    SlotMapping,
    StepGraph,
)
from easylink.implementation import Implementation, NullImplementation
from easylink.utilities import paths
from easylink.utilities.data_utils import load_yaml


class LayerState(ABC):
    def __init__(self, step: "Step"):
        self._step = step

    @abstractmethod
    def get_implementation_graph(self) -> ImplementationGraph:
        """Resolve the graph composed of Steps into a graph composed of Implementations."""
        pass

    @abstractmethod
    def get_implementation_edges(self, edge: Edge) -> List[Edge]:
        """Propagate edges of StepGraph to ImplementationGraph."""
        pass

    @abstractmethod
    def configure_step(
        self, step_config: LayeredConfigTree, input_data_config: LayeredConfigTree
    ) -> None:
        """Configure the step against the pipeline configuration and input data."""
        pass


class LeafState(LayerState):
    def configure_step(
        self, step_config: LayeredConfigTree, input_data_config: LayeredConfigTree
    ) -> None:
        pass

    def get_implementation_graph(self) -> ImplementationGraph:
        implementation_graph = ImplementationGraph()
        """Return a single node with an implementation attribute."""
        implementation_config = self._step.config["implementation"]
        implementation_node_name = self._step.implementation_node_name
        implementation = Implementation(
            step_name=self._step.step_name,
            implementation_config=implementation_config,
            input_slots=self._step.input_slots,
            output_slots=self._step.output_slots,
        )
        implementation_graph.add_node_from_impl(
            implementation_node_name,
            implementation=implementation,
        )
        return implementation_graph

    def get_implementation_edges(self, edge: Edge) -> List[Edge]:
        implementation_edges = []
        if edge.source_node == self._step.name:
            mappings = [
                mapping
                for mapping in self._step.implementation_slot_mappings()["output"]
                if mapping.parent_slot == edge.output_slot
            ]
            for mapping in mappings:
                imp_edge = mapping.propagate_edge(edge)
                implementation_edges.append(imp_edge)

        elif edge.target_node == self._step.name:
            mappings = [
                mapping
                for mapping in self._step.implementation_slot_mappings()["input"]
                if mapping.parent_slot == edge.input_slot
            ]
            for mapping in mappings:
                if (
                    "input_data_file" in self._step.config
                    and edge.source_node == "pipeline_graph_input_data"
                ):
                    edge.output_slot = self._step.config["input_data_file"]
                imp_edge = mapping.propagate_edge(edge)
                implementation_edges.append(imp_edge)
        else:
            raise ValueError(f"Step {self._step.name} not in edge {edge}")
        if not implementation_edges:
            raise ValueError(f"No edges found for Step {self._step.name} in edge {edge}")
        return implementation_edges


class CompositeState(LayerState):
    
    def __init__(self, step: "Step"):
        super().__init__(step)
        if not step.step_graph:
            raise ValueError(f"CompositeState requires a subgraph to operate on, but Step {step.name} has no step graph.")
        
    def get_implementation_graph(self) -> ImplementationGraph:
        """Call get_implementation_graph on each subgraph node and update the graph."""
        implementation_graph = ImplementationGraph()
        self.update_nodes(implementation_graph)
        self.update_edges(implementation_graph)
        return implementation_graph

    def update_nodes(self, implementation_graph: ImplementationGraph) -> None:
        for node in self._step.step_graph.nodes:
            step = self._step.step_graph.nodes[node]["step"]
            implementation_graph.update(step.get_implementation_graph())

    def update_edges(self, implementation_graph: ImplementationGraph) -> None:
        for source, target, edge_attrs in self._step.step_graph.edges(data=True):
            all_edges = []
            edge = Edge.from_graph_edge(source, target, edge_attrs)
            parent_source_step = self._step.step_graph.nodes[source]["step"]
            parent_target_step = self._step.step_graph.nodes[target]["step"]

            source_edges = parent_source_step.get_implementation_edges(edge)
            for source_edge in source_edges:
                for target_edge in parent_target_step.get_implementation_edges(source_edge):
                    all_edges.append(target_edge)

            for edge in all_edges:
                implementation_graph.add_edge_from_data(edge)

    def get_implementation_edges(self, edge: Edge) -> List[Edge]:
        implementation_edges = []
        if edge.source_node == self._step.name:
            mappings = [
                mapping
                for mapping in self._step.slot_mappings["output"]
                if mapping.parent_slot == edge.output_slot
            ]
            for mapping in mappings:
                new_edge = mapping.propagate_edge(edge)
                new_step = self._step.step_graph.nodes[mapping.child_node]["step"]
                imp_edges = new_step.get_implementation_edges(new_edge)
                implementation_edges.extend(imp_edges)

        elif edge.target_node == self._step.name:
            mappings = [
                mapping
                for mapping in self._step.slot_mappings["input"]
                if mapping.parent_slot == edge.input_slot
            ]
            for mapping in mappings:
                new_edge = mapping.propagate_edge(edge)
                new_step = self._step.step_graph.nodes[mapping.child_node]["step"]
                imp_edges = new_step.get_implementation_edges(new_edge)
                implementation_edges.extend(imp_edges)
        else:
            raise ValueError(f" {self.name} not in edge {edge}")
        if not implementation_edges:
            raise ValueError(f"No edges found for {self.name} in edge {edge}")
        return implementation_edges

    def configure_step(
        self, step_config: LayeredConfigTree, input_data_config: LayeredConfigTree
    ) -> None:
        for node in self._step.step_graph.nodes:
            step = self._step.step_graph.nodes[node]["step"]
            step.configure_step(self._step.config, input_data_config)


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
        self._layer_state = None

    @property
    def layer_state(self) -> LayerState:
        if self._layer_state is None:
            raise ValueError(f"Step {self.name}'s layer_state was invoked before being set")
        return self._layer_state

    @layer_state.setter
    def layer_state(self, state: LayerState) -> None:
        self._layer_state = state

    @property
    def config(self) -> LayeredConfigTree:
        if self._config is None:
            raise ValueError(f"Step {self.name}'s config was invoked before being set")
        return self._config

    @abstractmethod
    def validate_step(
        self, step_config: LayeredConfigTree, input_data_config: LayeredConfigTree
    ) -> Dict[str, List[str]]:
        """Validate the step against the pipeline configuration."""
        pass

    def get_implementation_graph(self) -> ImplementationGraph:
        return self.layer_state.get_implementation_graph()

    def get_implementation_edges(self, edge: Edge) -> List[Edge]:
        return self.layer_state.get_implementation_edges(edge)

    def set_parent_step(self, step) -> None:
        self.parent_step = step

    def set_step_config(self, parent_config: LayeredConfigTree) -> None:
        self._config = parent_config[self.name]

    def configure_step(
        self, step_config: LayeredConfigTree, input_data_config: LayeredConfigTree
    ) -> None:
        self.set_step_config(step_config)
        self.layer_state.configure_step(step_config, input_data_config)

    @property
    def implementation_node_name(self) -> str:
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

    def implementation_slot_mappings(self) -> Dict[str, List[SlotMapping]]:
        return {
            "input": [
                InputSlotMapping(slot, self.implementation_node_name, slot)
                for slot in self.input_slots
            ],
            "output": [
                OutputSlotMapping(slot, self.implementation_node_name, slot)
                for slot in self.output_slots
            ],
        }


class IOStep(Step):
    """"""

    def __init__(
        self,
        step_name: str,
        name: str = None,
        input_slots: List[InputSlot] = [],
        output_slots: List[OutputSlot] = [],
    ) -> None:
        super().__init__(step_name, name, input_slots, output_slots)
        self._layer_state = LeafState(self)

    @property
    def implementation_node_name(self):
        return "pipeline_graph_" + self.name

    def validate_step(
        self, step_config: LayeredConfigTree, input_data_config: LayeredConfigTree
    ) -> Dict[str, List[str]]:
        return {}

    def set_step_config(self, parent_config: LayeredConfigTree) -> None:
        self._config = parent_config

    def get_implementation_graph(self) -> ImplementationGraph:
        """Add a single node to the graph based on step name."""
        implementation_graph = ImplementationGraph()
        implementation_graph.add_node_from_impl(
            self.implementation_node_name,
            implementation=NullImplementation(
                self.implementation_node_name, self.input_slots, self.output_slots
            ),
        )
        return implementation_graph


class InputStep(IOStep):
    def configure_step(
        self, step_config: LayeredConfigTree, input_data_config: LayeredConfigTree
    ) -> None:
        """Configure the step against the pipeline configuration and input data."""
        super().configure_step(step_config, input_data_config)
        for input_data_key in input_data_config:
            self.output_slots[input_data_key] = OutputSlot(name=input_data_key)


class BasicStep(Step):
    """Step for leaf node tied to a specific single implementation"""

    def __init__(
        self,
        step_name: str,
        name: str = None,
        input_slots: List[InputSlot] = [],
        output_slots: List[OutputSlot] = [],
    ) -> None:
        super().__init__(step_name, name, input_slots, output_slots)
        self._layer_state = LeafState(self)

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
        self.step_graph = self._get_step_graph(nodes, edges)
        self.slot_mappings = slot_mappings
        self._layer_state = CompositeState(self)

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

    def _get_step_graph(self, nodes: List[Step], edges: List[Edge]) -> StepGraph:
        """Create a StepGraph from the nodes and edges the step was initialized with."""
        step_graph = StepGraph()
        for step in nodes:
            step_graph.add_node_from_step(step)
        for edge in edges:
            step_graph.add_edge_from_data(edge)
        return step_graph


class HierarchicalStep(CompositeStep):
    """A HierarchicalStep can be a single implementation or several 'substeps'. This requires
    a "substeps" key in the step configuration. If no substeps key is present, it will be treated as
    a single implemented step."""

    @property
    def config_key(self):
        return "substeps"

    def validate_step(
        self, step_config: LayeredConfigTree, input_data_config: LayeredConfigTree
    ) -> Dict[str, List[str]]:
        if not self.config_key in step_config:
            return BasicStep.validate_step(self, step_config, input_data_config)
        sub_config = step_config[self.config_key]
        return CompositeStep.validate_step(self, sub_config, input_data_config)

    def set_step_config(self, parent_config: LayeredConfigTree) -> None:
        step_config = parent_config[self.name]
        if self.config_key in step_config:
            self._config = step_config[self.config_key]
            self.layer_state = CompositeState(self)
        else:
            self._config = step_config
            self.layer_state = LeafState(self)


class LoopStep(Step):
    """A LoopStep allows a user to loop a single step or a sequence of steps a user-configured number of times."""

    def __init__(
        self,
        step_name: str,
        name: str = None,
        input_slots: List[InputSlot] = [],
        output_slots: List[OutputSlot] = [],
        template_step: Step = None,
        self_edges: List[Edge] = [],
    ) -> None:
        super().__init__(step_name, name, input_slots, output_slots)
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

    @property
    def num_repeats(self):
        return len(self.config)

    def validate_step(
        self, step_config: LayeredConfigTree, input_data_config: LayeredConfigTree
    ) -> Dict[str, List[str]]:
        if not self.config_key in step_config:
            return self.template_step.validate_step(step_config, input_data_config)

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

    def set_step_config(self, parent_config: LayeredConfigTree) -> None:
        step_config = parent_config[self.name]
        if not self.config_key in step_config:
            self._config = step_config
            self.layer_state = LeafState(self)
        else:
            self._config = self._get_expanded_config(step_config[self.config_key])
            self.step_graph = self._get_step_graph()
            self.slot_mappings = self._get_slot_mappings()
            self.layer_state = CompositeState(self)


    def _get_step_graph(self) -> StepGraph:
        """Make N copies of the iterated graph and chain them together according
        to the self edges."""
        graph = StepGraph()
        nodes = []
        edges = []

        for i in range(self.num_repeats):
            self.template_step.parent_step = None
            updated_step = copy.deepcopy(self.template_step)
            updated_step.set_parent_step(self)
            updated_step.name = f"{self.name}_loop_{i+1}"
            nodes.append(updated_step)
            if i > 0:
                for self_edge in self.self_edges:
                    source_node = f"{self.name}_loop_{i}"
                    target_node = f"{self.name}_loop_{i+1}"
                    edge = Edge(
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

    def _get_slot_mappings(self) -> dict:
        """Get the appropriate slot mappings based on the number of loops
        and the non-self-edge input and output slots."""
        input_mappings = []
        self_edge_input_slots = {edge.input_slot for edge in self.self_edges}
        external_input_slots = self.input_slots.keys() - self_edge_input_slots
        for input_slot in self_edge_input_slots:
            input_mappings.append(
                InputSlotMapping(input_slot, f"{self.name}_loop_1", input_slot)
            )
        for input_slot in external_input_slots:
            input_mappings.extend(
                [
                    InputSlotMapping(input_slot, f"{self.name}_loop_{n+1}", input_slot)
                    for n in range(self.num_repeats)
                ]
            )
        output_mappings = [
            OutputSlotMapping(slot, f"{self.name}_loop_{self.num_repeats}", slot)
            for slot in self.output_slots
        ]
        return {"input": input_mappings, "output": output_mappings}

    def _get_expanded_config(
        self, step_config: LayeredConfigTree
    ) -> Dict[str, LayeredConfigTree]:
        """Get the dictionary for the looped graph based on the sequence
        of sub-yamls."""
        expanded_config = {}
        for i, sub_config in enumerate(step_config):
            expanded_config[f"{self.name}_loop_{i+1}"] = sub_config
        return LayeredConfigTree(expanded_config)


class ParallelStep(Step):
    """A ParallelStep allows a user to run a sequence of steps in parallel."""

    def __init__(
        self,
        step_name: str,
        name: str = None,
        input_slots: List[InputSlot] = [],
        output_slots: List[OutputSlot] = [],
        template_step: Step = None,
    ) -> None:
        super().__init__(step_name, name, input_slots, output_slots)
        if not template_step or template_step.name != step_name:
            raise NotImplementedError(
                f"ParallelStep {self.name} must be initialized with a single node with the same name."
            )
        self.template_step = template_step
        self.template_step.set_parent_step(self)

    @property
    def config_key(self):
        return "parallel"

    @property
    def num_repeats(self):
        return len(self.config)

    def validate_step(
        self, step_config: LayeredConfigTree, input_data_config: LayeredConfigTree
    ) -> Dict[str, List[str]]:
        if not self.config_key in step_config:
            return self.template_step.validate_step(step_config, input_data_config)

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

    def set_step_config(self, parent_config: LayeredConfigTree) -> None:
        step_config = parent_config[self.name]
        if not self.config_key in step_config:
            self._config = step_config
            self.layer_state = LeafState(self)
        else:
            self._config = self._get_expanded_config(step_config[self.config_key])
            self.step_graph = self._get_step_graph()
            self.slot_mappings = self._get_slot_mappings()
            self.layer_state = CompositeState(self)

    def _get_step_graph(self) -> StepGraph:
        """Make N copies of the template step that are independent and contain the same edges as the
        current step"""
        graph = StepGraph()

        for i in range(self.num_repeats):
            self.template_step.parent_step = None
            updated_step = copy.deepcopy(self.template_step)
            updated_step.set_parent_step(self)
            updated_step.name = f"{self.name}_parallel_split_{i+1}"
            graph.add_node_from_step(updated_step)
        return graph

    def _get_slot_mappings(self) -> Dict[str, List[SlotMapping]]:
        """Get the appropriate slot mappings based on the number of parallel copies
        and the existing input and output slots."""
        input_mappings = [
            InputSlotMapping(slot, f"{self.name}_parallel_split_{n+1}", slot)
            for n in range(self.num_repeats)
            for slot in self.input_slots
        ]
        output_mappings = [
            OutputSlotMapping(slot, f"{self.name}_parallel_split_{n+1}", slot)
            for n in range(self.num_repeats)
            for slot in self.output_slots
        ]
        return {"input": input_mappings, "output": output_mappings}

    def _get_expanded_config(
        self, step_config: LayeredConfigTree
    ) -> Dict[str, LayeredConfigTree]:
        """Get the dictionary for the parallel graph based on the sequence
        of sub-yamls."""
        expanded_step_config = {}
        for i, sub_config in enumerate(step_config):
            expanded_step_config[f"{self.name}_parallel_split_{i+1}"] = sub_config
        return LayeredConfigTree(expanded_step_config)
