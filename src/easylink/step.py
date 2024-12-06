from __future__ import annotations

import copy
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Iterable
from typing import Any

from layered_config_tree import LayeredConfigTree

from easylink.graph_components import (
    EdgeParams,
    ImplementationGraph,
    InputSlot,
    InputSlotMapping,
    OutputSlot,
    OutputSlotMapping,
    SlotMapping,
    StepGraph,
)
from easylink.implementation import (
    Implementation,
    NullImplementation,
    PartialImplementation,
)
from easylink.utilities import paths
from easylink.utilities.data_utils import load_yaml

COMBINED_IMPLEMENTATION_KEY = "combined_implementation_key"


class ConfigurationState(ABC):
    """A given step's configuration state.

    A ConfigurationState defines the exact pipeline configuration state for a given step, including the
    strategy required to get the implementation graph from the step. There are two possible configured
    step states: "Leaf" and "Non-Leaf".
    """

    def __init__(
        self,
        step: Step,
        pipeline_config: LayeredConfigTree,
        combined_implementations: LayeredConfigTree,
        input_data_config: LayeredConfigTree,
    ):
        self._step = step
        self.pipeline_config = pipeline_config
        self.combined_implementations = combined_implementations
        self.input_data_config = input_data_config

    @abstractmethod
    def get_implementation_graph(self) -> ImplementationGraph:
        """Resolve the graph composed of Steps into a graph composed of Implementations."""
        pass

    @abstractmethod
    def get_implementation_edges(self, edge: EdgeParams) -> list[EdgeParams]:
        """Propagate edges of StepGraph to ImplementationGraph."""
        pass


class LeafConfigurationState(ConfigurationState):
    """A concrete instance of ConfigurationState for a leaf node in the pipeline graph.

    This corresponds to a leaf node in the pipeline graph that is implemented by a single implementation.
    """

    @property
    def is_combined(self) -> bool:
        return True if COMBINED_IMPLEMENTATION_KEY in self.pipeline_config else False

    @property
    def implementation_config(self) -> LayeredConfigTree:
        return (
            self.combined_implementations[self.pipeline_config[COMBINED_IMPLEMENTATION_KEY]]
            if self.is_combined
            else self.pipeline_config["implementation"]
        )

    def get_implementation_graph(self) -> ImplementationGraph:
        """Return a single node with an implementation attribute."""
        implementation_graph = ImplementationGraph()
        implementation_node_name = self._step.implementation_node_name
        if self.is_combined:
            implementation = PartialImplementation(
                combined_name=self.pipeline_config[COMBINED_IMPLEMENTATION_KEY],
                schema_step=self._step.step_name,
                input_slots=self._step.input_slots.values(),
                output_slots=self._step.output_slots.values(),
            )
        else:
            implementation = Implementation(
                schema_steps=[self._step.step_name],
                implementation_config=self.implementation_config,
                input_slots=self._step.input_slots.values(),
                output_slots=self._step.output_slots.values(),
            )
        implementation_graph.add_node_from_implementation(
            implementation_node_name,
            implementation=implementation,
        )
        return implementation_graph

    def get_implementation_edges(self, edge: EdgeParams) -> list[EdgeParams]:
        implementation_edges = []
        if edge.source_node == self._step.name:
            mappings = [
                mapping
                for mapping in self._step.implementation_slot_mappings()["output"]
                if mapping.parent_slot == edge.output_slot
            ]
            for mapping in mappings:
                imp_edge = mapping.remap_edge(edge)
                implementation_edges.append(imp_edge)

        elif edge.target_node == self._step.name:
            mappings = [
                mapping
                for mapping in self._step.implementation_slot_mappings()["input"]
                if mapping.parent_slot == edge.input_slot
            ]
            for mapping in mappings:
                if (
                    "input_data_file" in self.pipeline_config
                    and edge.source_node == "pipeline_graph_input_data"
                ):
                    edge.output_slot = self.pipeline_config["input_data_file"]
                imp_edge = mapping.remap_edge(edge)
                implementation_edges.append(imp_edge)
        else:
            raise ValueError(f"Step {self._step.name} not in edge {edge}")
        if not implementation_edges:
            raise ValueError(f"No edges found for Step {self._step.name} in edge {edge}")
        return implementation_edges


class NonLeafConfigurationState(ConfigurationState):
    """A concrete instance of ConfigurationState for a non-leaf node in the pipeline graph.

    This class type is selected when a step has a non-trivial step graph and has been configured
    to use the step graph (e.g. through a configuration key in the pipeline specification yaml).
    """

    def __init__(
        self,
        step: Step,
        pipeline_config: LayeredConfigTree,
        combined_implementations: LayeredConfigTree,
        input_data_config: LayeredConfigTree,
    ):
        super().__init__(step, pipeline_config, combined_implementations, input_data_config)
        if not step.step_graph:
            raise ValueError(
                f"NonLeafConfigurationState requires a subgraph upon which to operate, but Step {step.name} has no step graph."
            )
        self.configure_subgraph_steps()

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
            edge = EdgeParams.from_graph_edge(source, target, edge_attrs)
            parent_source_step = self._step.step_graph.nodes[source]["step"]
            parent_target_step = self._step.step_graph.nodes[target]["step"]

            source_edges = parent_source_step.get_implementation_edges(edge)
            for source_edge in source_edges:
                for target_edge in parent_target_step.get_implementation_edges(source_edge):
                    all_edges.append(target_edge)

            for edge in all_edges:
                implementation_graph.add_edge_from_params(edge)

    def get_implementation_edges(self, edge: EdgeParams) -> list[EdgeParams]:
        implementation_edges = []
        if edge.source_node == self._step.name:
            mappings = [
                mapping
                for mapping in self._step.slot_mappings["output"]
                if mapping.parent_slot == edge.output_slot
            ]
            for mapping in mappings:
                new_edge = mapping.remap_edge(edge)
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
                new_edge = mapping.remap_edge(edge)
                new_step = self._step.step_graph.nodes[mapping.child_node]["step"]
                imp_edges = new_step.get_implementation_edges(new_edge)
                implementation_edges.extend(imp_edges)
        else:
            raise ValueError(f" {self._step.name} not in edge {edge}")
        if not implementation_edges:
            raise ValueError(f"No edges found for {self._step.name} in edge {edge}")
        return implementation_edges

    def configure_subgraph_steps(self) -> None:
        for node in self._step.step_graph.nodes:
            step = self._step.step_graph.nodes[node]["step"]
            step.set_configuration_state(
                self.pipeline_config, self.combined_implementations, self.input_data_config
            )


class Step:
    """The highest-level pipeline building block abstraction.

    Steps contain information about the purpose of the interoperable elements of
    the sequence called a *Pipeline* and how those elements relate to one another.
    In turn, steps are implemented by Implementations, such that each step may have
    several implementations but each implementation must have exactly one step.
    In a sense, steps contain metadata about the implementations to which they relate.
    """

    def __init__(
        self,
        step_name: str,
        name: str | None = None,
        input_slots: Iterable[InputSlot] = (),
        output_slots: Iterable[OutputSlot] = (),
        nodes: Iterable[Step] = (),
        edges: Iterable[EdgeParams] = (),
        input_slot_mappings: Iterable[InputSlotMapping] = (),
        output_slot_mappings: Iterable[OutputSlotMapping] = (),
    ) -> None:
        self.step_name = step_name
        # TODO: try removing name as an arg and just set self.name = self.step_name
        self.name = name if name else step_name
        self.input_slots = {slot.name: slot for slot in input_slots}
        self.output_slots = {slot.name: slot for slot in output_slots}
        self.nodes = nodes
        for node in self.nodes:
            node.set_parent_step(self)
        self.edges = edges
        self.step_graph = self._get_step_graph(nodes, edges)
        self.slot_mappings = {
            "input": list(input_slot_mappings),
            "output": list(output_slot_mappings),
        }
        self.parent_step = None
        self._configuration_state = None

    @property
    def config_key(self):
        return None

    @property
    def configuration_state(self) -> ConfigurationState:
        if self._configuration_state is None:
            raise ValueError(
                f"Step {self.name}'s configuration_state was invoked before being set"
            )
        return self._configuration_state

    def validate_leaf(
        self,
        step_config: LayeredConfigTree,
        combined_implementations: LayeredConfigTree,
    ) -> dict[str, list[str]]:
        errors = {}
        metadata = load_yaml(paths.IMPLEMENTATION_METADATA)
        if (
            "implementation" not in step_config
            and COMBINED_IMPLEMENTATION_KEY not in step_config
        ):
            errors[f"step {self.name}"] = [
                "The step configuration does not contain an 'implementation' key or a "
                "reference to a combined implementation."
            ]
        elif (
            COMBINED_IMPLEMENTATION_KEY in step_config
            and not step_config[COMBINED_IMPLEMENTATION_KEY] in combined_implementations
        ):
            errors[f"step {self.name}"] = [
                f"The step refers to a combined implementation but {step_config[COMBINED_IMPLEMENTATION_KEY]} is not a "
                f"valid combined implementation."
            ]
        else:
            implementation_config = (
                step_config["implementation"]
                if "implementation" in step_config
                else combined_implementations[step_config[COMBINED_IMPLEMENTATION_KEY]]
            )
            if not "name" in implementation_config:
                errors[f"step {self.name}"] = [
                    "The implementation configuration does not contain a 'name' key."
                ]
            elif not implementation_config["name"] in metadata:
                errors[f"step {self.name}"] = [
                    f"Implementation '{implementation_config['name']}' is not supported. "
                    f"Supported implementations are: {list(metadata.keys())}."
                ]
        return errors

    def validate_nonleaf(
        self,
        step_config: LayeredConfigTree,
        combined_implementations: LayeredConfigTree,
        input_data_config: LayeredConfigTree,
    ) -> dict[str, list[str]]:
        errors = {}
        for node in self.step_graph.nodes:
            step = self.step_graph.nodes[node]["step"]
            if isinstance(step, IOStep):
                continue
            if step.name not in step_config:
                step_errors = {f"step {step.name}": [f"The step is not configured."]}
            else:
                step_errors = step.validate_step(
                    step_config[step.name], combined_implementations, input_data_config
                )
            if step_errors:
                errors.update(step_errors)
        extra_steps = set(step_config.keys()) - set(self.step_graph.nodes)
        for extra_step in extra_steps:
            errors[f"step {extra_step}"] = [f"{extra_step} is not a valid step."]
        return errors

    def validate_step(
        self,
        step_config: LayeredConfigTree,
        combined_implementations: LayeredConfigTree,
        input_data_config: LayeredConfigTree,
    ) -> dict[str, list[str]]:
        if len(self.step_graph.nodes) == 0:
            return self.validate_leaf(step_config, combined_implementations)
        elif self.config_key in step_config:
            return self.validate_nonleaf(
                step_config[self.config_key], combined_implementations, input_data_config
            )
        else:
            return self.validate_leaf(step_config, combined_implementations)

    def get_implementation_graph(self) -> ImplementationGraph:
        return self.configuration_state.get_implementation_graph()

    def get_implementation_edges(self, edge: EdgeParams) -> list[EdgeParams]:
        return self.configuration_state.get_implementation_edges(edge)

    def set_parent_step(self, step: Step) -> None:
        self.parent_step = step

    def get_state_config(self, step_config: LayeredConfigTree) -> None:
        return (
            step_config
            if not self.config_key in step_config
            else step_config[self.config_key]
        )

    def set_configuration_state(
        self,
        parent_config: LayeredConfigTree,
        combined_implementations: LayeredConfigTree,
        input_data_config: LayeredConfigTree,
    ) -> None:
        step_config = parent_config[self.name]
        state_config = self.get_state_config(step_config)
        if self.config_key is not None and self.config_key in step_config:
            self._configuration_state = NonLeafConfigurationState(
                self, state_config, combined_implementations, input_data_config
            )
        else:
            self._configuration_state = LeafConfigurationState(
                self, state_config, combined_implementations, input_data_config
            )

    def _get_step_graph(self, nodes: list[Step], edges: list[EdgeParams]) -> StepGraph:
        """Create a StepGraph from the nodes and edges the step was initialized with."""
        step_graph = StepGraph()
        for step in nodes:
            step_graph.add_node_from_step(step)
        for edge in edges:
            step_graph.add_edge_from_params(edge)
        return step_graph

    @property
    def implementation_node_name(self) -> str:
        """Determines a sensible unique node name for the implementation graph.

        This method compares the step node names with the step names through the step hierarchy and
        uses the full suffix of step names starting from wherever the two first differ. For example,
        loop steps may have multiple loops with the same implementation and step, e.g. "step_3" and "step_3_python_pandas".
        If we have node names step_3_loop_1 step_3_python_pandas the resulting implementation node name
        will be step_3_loop_1_step_3_python_pandas. If all the node names and step names match, we have not
        introduced any step degeneracies with e.g. loops or multiples, and we can simply use the implementation
        name.
        """
        step = self
        implementation_name = (
            self.configuration_state.pipeline_config[COMBINED_IMPLEMENTATION_KEY]
            if self.configuration_state.is_combined
            else self.configuration_state.implementation_config.name
        )
        node_names = []
        step_names = []
        while step:
            node_names.append(step.name)
            step_names.append(step.step_name)
            step = step.parent_step

        prefix = []
        step_names.reverse()
        node_names.reverse()
        for i, (step_name, node_name) in enumerate(zip(step_names, node_names)):
            if step_name != node_name:
                prefix = node_names[i:]
                break
        # If we didn't include the step name already for a combined implementation,
        # do so now.
        if self.configuration_state.is_combined and not prefix:
            prefix.append(self.name)
        prefix.append(implementation_name)
        return "_".join(prefix)

    def implementation_slot_mappings(self) -> dict[str, list[SlotMapping]]:
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

    @property
    def implementation_node_name(self) -> str:
        return self.name

    def validate_step(
        self,
        step_config: LayeredConfigTree,
        combined_implementations: LayeredConfigTree,
        input_data_config: LayeredConfigTree,
    ) -> dict[str, list[str]]:
        return {}

    def set_configuration_state(
        self,
        parent_config: LayeredConfigTree,
        combined_implementations: LayeredConfigTree,
        input_data_config: LayeredConfigTree,
    ) -> None:
        self._configuration_state = LeafConfigurationState(
            self, parent_config, combined_implementations, input_data_config
        )

    def get_implementation_graph(self) -> ImplementationGraph:
        """Add a single node to the graph based on step name."""
        implementation_graph = ImplementationGraph()
        implementation_graph.add_node_from_implementation(
            self.name,
            implementation=NullImplementation(
                self.name, self.input_slots.values(), self.output_slots.values()
            ),
        )
        return implementation_graph


class InputStep(IOStep):
    def __init__(
        self,
        output_slots: Iterable[OutputSlot] = (OutputSlot("all"),),
    ) -> None:
        super().__init__(step_name="input_data", output_slots=output_slots)

    def set_configuration_state(
        self,
        parent_config: LayeredConfigTree,
        combined_implementations: LayeredConfigTree,
        input_data_config: LayeredConfigTree,
    ) -> None:
        """Configure the step against the pipeline configuration and input data."""
        super().set_configuration_state(
            parent_config, combined_implementations, input_data_config
        )
        for input_data_key in input_data_config:
            self.output_slots[input_data_key] = OutputSlot(name=input_data_key)


class OutputStep(IOStep):
    def __init__(self, input_slots: Iterable[InputSlot]) -> None:
        super().__init__("results", input_slots=input_slots)


class HierarchicalStep(Step):
    """An abstraction to represent a step that may contain substeps.

    A HierarchicalStep can be a single implementation or several 'substeps'. This requires
    a "substeps" key in the step configuration. If no substeps key is present, it will be treated as
    a single implemented step.
    """

    @property
    def config_key(self):
        return "substeps"


class TemplatedStep(Step):
    """A helper class to represent a step that potentially contains some multiplicity.

    Some steps may contain a specified amount of multiplicity, such as a loop or parallel split.
    This class is a helper class intended to be subclassed by discrete concrete instances
    of these multiples according to a specified transformation rule and the user-specified configuration.
    """

    def __init__(
        self,
        template_step: Step,
    ) -> None:
        super().__init__(
            template_step.step_name,
            template_step.name,
            template_step.input_slots.values(),
            template_step.output_slots.values(),
        )
        self.template_step = template_step
        self.template_step.set_parent_step(self)

    @property
    @abstractmethod
    def node_prefix(self) -> str:
        pass

    @abstractmethod
    def _update_step_graph(self, state_config) -> StepGraph:
        pass

    @abstractmethod
    def _update_slot_mappings(self, state_config) -> dict[str, list[SlotMapping]]:
        """Get the appropriate slot mappings based on the number of parallel copies
        and the existing input and output slots."""
        pass

    def validate_step(
        self,
        step_config: LayeredConfigTree,
        combined_implementations: LayeredConfigTree,
        input_data_config: LayeredConfigTree,
    ) -> dict[str, list[str]]:
        if not self.config_key in step_config:
            return self.template_step.validate_step(
                step_config, combined_implementations, input_data_config
            )

        sub_config = step_config[self.config_key]

        if not isinstance(sub_config, list):
            return {
                f"step {self.name}": [
                    f"{self.node_prefix.capitalize()} instances must be formatted as a sequence in the pipeline configuration."
                ]
            }

        if len(sub_config) == 0:
            return {
                f"step {self.name}": [
                    f"No {self.node_prefix} instances configured under '{self.config_key}' key."
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
                self.template_step.validate_step(
                    parallel_config, combined_implementations, input_data_config
                )
            )
            if parallel_errors:
                errors[f"step {self.name}"][f"{self.node_prefix}_{i+1}"] = parallel_errors
        return errors

    def get_state_config(self, step_config: LayeredConfigTree) -> None:
        if self.config_key in step_config:
            expanded_step_config = LayeredConfigTree()
            for i, sub_config in enumerate(step_config[self.config_key]):
                expanded_step_config.update(
                    {f"{self.name}_{self.node_prefix}_{i+1}": sub_config}
                )
            return expanded_step_config
        return step_config

    def set_configuration_state(
        self,
        parent_config: LayeredConfigTree,
        combined_implementations: LayeredConfigTree,
        input_data_config: LayeredConfigTree,
    ):
        num_repeats = len(self.get_state_config(parent_config[self.name]))
        self.step_graph = self._update_step_graph(num_repeats)
        self.slot_mappings = self._update_slot_mappings(num_repeats)
        super().set_configuration_state(
            parent_config, combined_implementations, input_data_config
        )


class LoopStep(TemplatedStep):
    """A LoopStep allows a user to loop a single step or a sequence of steps multiple times."""

    def __init__(
        self,
        template_step: Step | None = None,
        self_edges: Iterable[EdgeParams] = (),
    ) -> None:
        super().__init__(template_step)
        self.self_edges = self_edges

    @property
    def config_key(self):
        return "iterate"

    @property
    def node_prefix(self):
        return "loop"

    def _update_step_graph(self, num_repeats) -> StepGraph:
        """Makes N copies of the iterated graph and chain them together according to the self edges."""
        graph = StepGraph()
        nodes = []
        edges = []

        for i in range(num_repeats):
            self.template_step.parent_step = None
            updated_step = copy.deepcopy(self.template_step)
            updated_step.set_parent_step(self)
            updated_step.name = f"{self.name}_{self.node_prefix}_{i+1}"
            nodes.append(updated_step)
            if i > 0:
                for self_edge in self.self_edges:
                    source_node = f"{self.name}_{self.node_prefix}_{i}"
                    target_node = f"{self.name}_{self.node_prefix}_{i+1}"
                    edge = EdgeParams(
                        source_node=source_node,
                        target_node=target_node,
                        input_slot=self_edge.input_slot,
                        output_slot=self_edge.output_slot,
                    )
                    edges.append(edge)

        for node in nodes:
            graph.add_node_from_step(node)
        for edge in edges:
            graph.add_edge_from_params(edge)
        return graph

    def _update_slot_mappings(self, num_repeats) -> dict:
        """Gets the appropriate slot mappings based on the number of loops and the non-self-edge input and output slots."""
        input_mappings = []
        self_edge_input_slots = {edge.input_slot for edge in self.self_edges}
        external_input_slots = self.input_slots.keys() - self_edge_input_slots
        for input_slot in self_edge_input_slots:
            input_mappings.append(
                InputSlotMapping(input_slot, f"{self.name}_{self.node_prefix}_1", input_slot)
            )
        for input_slot in external_input_slots:
            input_mappings.extend(
                [
                    InputSlotMapping(
                        input_slot, f"{self.name}_{self.node_prefix}_{n+1}", input_slot
                    )
                    for n in range(num_repeats)
                ]
            )
        output_mappings = [
            OutputSlotMapping(slot, f"{self.name}_{self.node_prefix}_{num_repeats}", slot)
            for slot in self.output_slots
        ]
        return {"input": input_mappings, "output": output_mappings}


class ParallelStep(TemplatedStep):
    """A ParallelStep allows a user to run a sequence of steps in parallel."""

    @property
    def config_key(self):
        return "parallel"

    @property
    def node_prefix(self):
        return "parallel_split"

    def _update_step_graph(self, num_repeats: int) -> StepGraph:
        """Makes N copies of the template step that are independent and contain the same edges as the current step"""
        graph = StepGraph()

        for i in range(num_repeats):
            self.template_step.parent_step = None
            updated_step = copy.deepcopy(self.template_step)
            updated_step.set_parent_step(self)
            updated_step.name = f"{self.name}_{self.node_prefix}_{i+1}"
            graph.add_node_from_step(updated_step)
        return graph

    def _update_slot_mappings(self, num_repeats: int) -> dict[str, list[SlotMapping]]:
        """Gets the appropriate slot mappings based on the number of parallel copies and the existing input and output slots."""
        input_mappings = [
            InputSlotMapping(slot, f"{self.name}_{self.node_prefix}_{n+1}", slot)
            for n in range(num_repeats)
            for slot in self.input_slots
        ]
        output_mappings = [
            OutputSlotMapping(slot, f"{self.name}_{self.node_prefix}_{n+1}", slot)
            for n in range(num_repeats)
            for slot in self.output_slots
        ]
        return {"input": input_mappings, "output": output_mappings}


class ChoiceStep(Step):
    """A ChoiceStep allows a user to select a single path from a set of possible paths."""

    def __init__(
        self,
        step_name: str,
        input_slots: Iterable[InputSlot],
        output_slots: Iterable[OutputSlot],
        choices: dict[
            str, dict[str, list[Step | EdgeParams | InputSlotMapping | OutputSlotMapping]]
        ],
    ) -> None:
        super().__init__(
            step_name,
            input_slots=input_slots,
            output_slots=output_slots,
        )
        self.choices = choices

    def validate_step(
        self,
        step_config: LayeredConfigTree,
        combined_implementations: LayeredConfigTree,
        input_data_config: LayeredConfigTree,
    ) -> dict[str, list[str]]:
        """Validates the ChoiceStep.

        Notes
        -----
        We update the step graph and slot mappings here in validation as opposed to
        in set_configuration_state (as is done in TemplatedSteps) because ChoiceStep
        validation happens prior to set_configuration_state and actually requires
        the step graph and mappings.

        We do not attempt to validate the subgraph here if the 'type' key is unable
        to be validated.
        """

        chosen_type = step_config.get("type")
        # Handle problems with the 'type' key
        if not chosen_type:
            return {f"step {self.name}": ["The step requires a 'type' key."]}
        if chosen_type not in self.choices:
            return {
                f"step {self.name}": [
                    f"'{step_config['type']}' is not a supported 'type'. Valid choices are: {list(self.choices)}."
                ]
            }
        # Handle type-subgraph inconsistencies
        subgraph = self.choices[chosen_type]
        chosen_step_config = LayeredConfigTree(
            {key: value for key, value in step_config.items() if key != "type"}
        )
        allowable_steps = [node.name for node in subgraph["nodes"]]
        if set(allowable_steps) != set(chosen_step_config):
            return {
                f"step {self.name}": [
                    f"Invalid configuration for '{chosen_type}' type. Valid steps are {allowable_steps}."
                ]
            }

        # Handle the actual chosen step_config
        self.step_graph = self._update_step_graph(subgraph)
        self.slot_mappings = self._update_slot_mappings(subgraph)
        # NOTE: A ChoiceStep is by definition non-leaf step
        return self.validate_nonleaf(
            chosen_step_config, combined_implementations, input_data_config
        )

    def set_configuration_state(
        self,
        parent_config: LayeredConfigTree,
        combined_implementations: LayeredConfigTree,
        input_data_config: LayeredConfigTree,
    ):
        """Sets the configuration state for a ChoiceStep.

        Notes
        -----
        We update the step graph and slot mappings validate_step as opposed to here
        (as is done in TemplatedSteps) because ChoiceStep validation happens prior
        this but actually requires the step graph and mappings.
        """

        chosen_parent_config = LayeredConfigTree(
            {key: value for key, value in parent_config[self.name].items() if key != "type"}
        )
        # ChoiceSteps by definition cannot be in a LeafConfigurationState.
        self._configuration_state = NonLeafConfigurationState(
            self, chosen_parent_config, combined_implementations, input_data_config
        )

    @staticmethod
    def _update_step_graph(subgraph: dict[str, Any]):
        nodes = subgraph["nodes"]
        edges = subgraph["edges"]

        graph = StepGraph()
        for node in nodes:
            graph.add_node_from_step(node)
        for edge in edges:
            graph.add_edge_from_params(edge)
        return graph

    @staticmethod
    def _update_slot_mappings(subgraph: dict[str, Any]) -> dict[str, list[SlotMapping]]:
        input_mappings = subgraph["input_slot_mappings"]
        output_mappings = subgraph["output_slot_mappings"]
        return {"input": input_mappings, "output": output_mappings}
