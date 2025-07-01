"""

This module is responsible for defining the abstractions that represent desired
steps to run in a pipeline. These so-called "steps" are high level and do not indicate
how they are to actually be implemented.

"""

from __future__ import annotations

import copy
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Callable, Iterable

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
    NullAggregatorImplementation,
    NullImplementation,
    NullSplitterImplementation,
    PartialImplementation,
)
from easylink.utilities import paths
from easylink.utilities.data_utils import load_yaml

COMBINED_IMPLEMENTATION_KEY = "combined_implementation_key"


class Step:
    """The highest-level pipeline building block abstraction.

    ``Steps`` contain information about the purpose of the interoperable tasks in
    the sequence called a "pipeline" and how those tasks relate to one another.
    In turn, ``Steps`` are implemented by :class:`Implementations<easylink.implementation.Implementation>`,
    such that each ``Step`` may have several ``Implementations`` to choose from
    but each ``Implementation`` must implemement exactly one ``Step``. As such, the
    pipeline for a given EasyLink run consists of ``Implementations`` that collectively
    span the ``Steps`` in the :class:`~easylink.pipeline_schema.PipelineSchema`.

    Parameters
    ----------
    step_name
        The name of the pipeline step in the ``PipelineSchema``. It must also match
        the key in the implementation metadata file to be used to run this ``Step``.
    name
        The name of this ``Step's`` node in its :class:`easylink.graph_components.StepGraph`.
        This can be different from the ``step_name`` due to the need for disambiguation
        during the process of flattening the ``Stepgraph``, e.g. unrolling loops, etc.
        For example, if step 1 is looped multiple times, each node would have a
        ``step_name`` of, perhaps, "step_1" but unique ``names`` ("step_1_loop_1", etc).
    input_slots
        All required :class:`InputSlots<easylink.graph_components.InputSlot>`.
    output_slots
        All required :class:`OutputSlots<easylink.graph_components.OutputSlot>`.
    input_slot_mappings
        The :class:`InputSlotMapping<easylink.graph_components.InputSlotMapping>` of this ``Step``.
    output_slot_mappings
        The :class:`OutputSlotMapping<easylink.graph_components.OutputSlotMapping>` of this ``Step``.
    is_auto_parallel
        Whether or not this ``Step`` is to automatically run in parallel.

    Notes
    -----
    This is the most basic type of step object available in the pipeline; it
    represents a single element of work to be run one time in the pipeline. Other
    classes inherit from this and expand upon it to represent more complex structures,
    e.g. to loop a step multiple times or to run multiple steps in parallel.

    """

    def __init__(
        self,
        step_name: str | None,
        name: str | None = None,
        input_slots: Iterable[InputSlot] = (),
        output_slots: Iterable[OutputSlot] = (),
        input_slot_mappings: Iterable[InputSlotMapping] = (),
        output_slot_mappings: Iterable[OutputSlotMapping] = (),
        is_auto_parallel: bool = False,
        default_implementation: str | None = None,
    ) -> None:
        if not step_name and not name:
            raise ValueError("All Steps must contain a step_name, name, or both.")
        self.step_name = step_name
        """The name of the pipeline step in the ``PipelineSchema``. It must also match
        the key in the implementation metadata file to be used to run this ``Step``."""
        self._name = name if name else step_name
        """The name of this ``Step's`` node in its :class:`easylink.graph_components.StepGraph`. 
        This can be different from the ``step_name`` due to the need for disambiguation 
        during the process of flattening the ``Stepgraph``, e.g. unrolling loops, etc. 
        For example, if step 1 is looped multiple times, each node would have a 
        ``step_name`` of, perhaps, "step_1" but unique ``names`` ("step_1_loop_1", etc)."""

        if len(set(slot.name for slot in input_slots)) != len(input_slots):
            raise ValueError(f"{step_name} has duplicate input slot names!")

        if len(set(s.env_var for s in input_slots)) != len(input_slots):
            raise ValueError(f"{step_name} has duplicate input slot environment variables!")

        self.input_slots = {slot.name: slot for slot in input_slots}
        """A mapping of ``InputSlot`` names to their instances."""

        if len(set(s.name for s in output_slots)) != len(output_slots):
            raise ValueError(f"{step_name} has duplicate output slot names!")

        self.output_slots = {slot.name: slot for slot in output_slots}
        """A mapping of ``OutputSlot`` names to their instances."""
        self.slot_mappings = {
            "input": list(input_slot_mappings),
            "output": list(output_slot_mappings),
        }
        """A combined dictionary containing both the ``InputSlotMappings`` and
        ``OutputSlotMappings`` of this ``Step``."""
        self.is_auto_parallel = is_auto_parallel
        """Whether or not this ``Step`` is to be automatically run in parallel."""
        self.default_implementation = default_implementation
        """The default implementation to use for this ``Step`` if the ``Step`` is
        not explicitly configured in the pipeline specification."""
        self.parent_step = None
        """This ``Step's`` parent ``Step``, if applicable."""
        self._configuration_state = None
        """This ``Step's`` :class:`~easylink.step.ConfigurationState`."""

    @property
    def name(self):
        """The name of this ``Step's`` node in its :class:`easylink.graph_components.StepGraph`.
        This can be different from the ``step_name`` due to the need for disambiguation
        during the process of flattening the ``Stepgraph``, e.g. unrolling loops, etc.
        For example, if step 1 is looped multiple times, each node would have a
        ``step_name`` of, perhaps, "step_1" but unique ``names`` ("step_1_loop_1", etc)."""
        return self._name

    @name.setter
    def name(self, value: str):
        """Sets the ``name`` of this ``Step``."""
        self._name = value

    @property
    def config_key(self):
        """The configuration key pertinent to this type of ``Step``."""
        return None

    @property
    def configuration_state(self) -> ConfigurationState:
        """The :class:`~easylink.step.ConfigurationState` of this ``Step``."""
        if self._configuration_state is None:
            raise ValueError(
                f"Step {self.name}'s configuration_state was invoked before being set"
            )
        return self._configuration_state

    @property
    def implementation_node_name(self) -> str:
        """The unique name to be used for this ``Step's`` node in the :class:`~easylink.graph_components.ImplementationGraph`.

        This compares the ``Step`` *instance* name to its *node* name via the ``Step's``
        ordered hierarchy of sub-``Steps`` and uses the full suffix of names starting
        from wherever the two first differ.

        For example, a ``Step`` named "step_3" may loop multiple times using the same
        :class:`~easylink.implementation.Implementation` named "step_3_python_pandas".
        However, to disambiguate between the different loops of "step_3", we might
        designate the node name to be "step_3_loop_1" and then combine that with the
        ``Implementation`` name such that the ``Implementation's`` node name is
        "step_3_loop_1_step_3_python_pandas".

        If all the node names and step names match, we have not introduced any step
        degeneracies (with e.g. loops or multiples), and we can simply use the
        implementation name directly.

        Returns
        -------
            The unique name to be used for this ``Step's`` node in the ``ImplementationGraph``.
        """
        step = self
        implementation_name = (
            self.configuration_state.step_config[COMBINED_IMPLEMENTATION_KEY]
            if self.configuration_state.is_combined
            else self.configuration_state.implementation_config.name
        )
        node_names = []
        step_names = []
        while step:
            if step.step_name:
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
        # If we didn't include the step name already for a combined implementation, do so now.
        if self.configuration_state.is_combined and not prefix:
            prefix.append(self.name)
        prefix.append(implementation_name)
        return "_".join(prefix)

    ###########
    # Methods #
    ###########

    def validate_step(
        self,
        step_config: LayeredConfigTree,
        combined_implementations: LayeredConfigTree,
        input_data_config: LayeredConfigTree,
    ) -> dict[str, list[str]]:
        """Validates the ``Step``.

        Parameters
        ----------
        step_config
            The internal configuration of this ``Step``, i.e. it should not include
            the ``Step's`` name.
        combined_implementations
            The configuration for any implementations to be combined.
        input_data_config
            The input data configuration for the entire pipeline.

        Returns
        -------
            A dictionary of errors, where the keys are the ``Step`` name and the
            values are lists of error messages associated with the given ``Step``.

        Notes
        -----
        If the ``Step`` does not validate (i.e. errors are found and the returned
        dictionary is non-empty), the tool will exit and the pipeline will not run.

        We attempt to batch error messages as much as possible, but there may be
        times where the configuration is so ill-formed that we are unable to handle
        all issues in one pass. In these cases, new errors may be found after the
        initial ones are handled.
        """
        errors = {}
        metadata = load_yaml(paths.IMPLEMENTATION_METADATA)
        error_key = f"step {self.name}"
        if (
            "implementation" not in step_config
            and COMBINED_IMPLEMENTATION_KEY not in step_config
        ):
            errors[error_key] = [
                "The step configuration does not contain an 'implementation' key "
                "or a reference to a combined implementation."
            ]
        elif (
            COMBINED_IMPLEMENTATION_KEY in step_config
            and not step_config[COMBINED_IMPLEMENTATION_KEY] in combined_implementations
        ):
            errors[error_key] = [
                "The step refers to a combined implementation but "
                f"{step_config[COMBINED_IMPLEMENTATION_KEY]} is not a valid combined "
                "implementation."
            ]
        else:
            implementation_config = (
                step_config["implementation"]
                if "implementation" in step_config
                else combined_implementations[step_config[COMBINED_IMPLEMENTATION_KEY]]
            )
            if not "name" in implementation_config:
                errors[error_key] = [
                    "The implementation configuration does not contain a 'name' key."
                ]
            elif not implementation_config["name"] in metadata:
                errors[error_key] = [
                    f"Implementation '{implementation_config['name']}' is not supported. "
                    f"Supported implementations are: {list(metadata.keys())}."
                ]
        return errors

    def add_nodes_to_implementation_graph(
        self, implementation_graph: ImplementationGraph
    ) -> None:
        """Adds the ``Implementations`` related to this ``Step`` as nodes to the :class:`~easylink.graph_components.ImplementationGraph`.

        How the nodes get added depends on whether this ``Step`` is a leaf or a non-leaf,
        i.e. what its :attr:`configuration_state` is.
        """
        self.configuration_state.add_nodes_to_implementation_graph(implementation_graph)

    def add_edges_to_implementation_graph(
        self, implementation_graph: ImplementationGraph
    ) -> None:
        """Adds the edges of this ``Step's`` ``Implementation(s)`` to the :class:`~easylink.graph_components.ImplementationGraph`.

        How the edges get added depends on whether this ``Step`` is a leaf or a non-leaf,
        i.e. what its :attr:`configuration_state` is.
        """
        self.configuration_state.add_edges_to_implementation_graph(implementation_graph)

    def get_implementation_edges(self, edge: EdgeParams) -> list[EdgeParams]:
        """Gets the edge information for the ``Implementation`` related to this ``Step``.

        Parameters
        ----------
        edge
            The ``Step's`` edge information to be propagated to the ``ImplementationGraph``.

        Returns
        -------
            The ``Implementation's`` edge information based on this ``Step's`` configuration
            state.
        """
        return self.configuration_state.get_implementation_edges(edge)

    def set_parent_step(self, step: Step) -> None:
        """Sets the parent of this ``Step``.

        Parameters
        ----------
        step
            The parent ``Step`` to be set for this instance's :attr:`parent_step`.
        """
        self.parent_step = step

    def set_configuration_state(
        self,
        step_config: LayeredConfigTree,
        combined_implementations: LayeredConfigTree,
        input_data_config: LayeredConfigTree,
    ) -> None:
        """Sets the configuration state to 'leaf'.

        Parameters
        ----------
        step_config
            The internal configuration of this ``Step``, i.e. it should not include
            the ``Step's`` name.
        combined_implementations
            The configuration for any implementations to be combined.
        input_data_config
            The input data configuration for the entire pipeline.
        """
        self._configuration_state = LeafConfigurationState(
            self, step_config, combined_implementations, input_data_config
        )

    def get_implementation_slot_mappings(self) -> dict[str, list[SlotMapping]]:
        """Gets the input and output :class:`SlotMappings<easylink.graph_components.SlotMapping>`."""
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


class StandaloneStep(Step, ABC):
    """A special case type of :class:`Step` that is not implemented on the pipeline.

    These are not typical ``Steps`` in that they do not represent a unit of work
    to be performed in the pipeline (i.e. there is no container to run) and,
    thus, are not implemented by an :class:`~easylink.implementation.Implementation`.

    See :class:`Step` for inherited attributes.

    """

    @property
    def implementation_node_name(self) -> str:
        """Dummy name to allow ``StandaloneSteps`` to be used interchangeably with other ``Steps``.

        Unlike other types of ``Steps``, ``StandaloneSteps`` are not actually implemented
        via an :class:`~easylink.implementation.Implementation` and thus do not
        require a different node name than its own ``Step`` name. This property
        only exists so that ``StandaloneSteps`` can be used interchangeably with other
        ``Steps`` in the codebase.

        Returns
        -------
            The ``StandaloneStep's`` name.
        """
        return self.name

    @abstractmethod
    def add_nodes_to_implementation_graph(
        self, implementation_graph: ImplementationGraph
    ) -> None:
        """Adds this ``StandaloneStep's`` ``Implementation`` as a node to the :class:`~easylink.graph_components.ImplementationGraph`.

        Notes
        -----
        Unlike other types of ``Steps``, ``StandaloneSteps`` are not actually implemented
        via an :class:`~easylink.implementation.Implementation`. As such, we
        leverage the :class:`~easylink.implementation.NullImplementation` class
        to generate the graph node.
        """
        pass

    def validate_step(
        self,
        step_config: LayeredConfigTree,
        combined_implementations: LayeredConfigTree,
        input_data_config: LayeredConfigTree,
    ) -> dict[str, list[str]]:
        """Dummy validation method to allow ``StandaloneSteps`` to be used interchangeably with other ``Steps``.

        Unlike other types of ``Steps``, ``StandaloneSteps`` are not actually implemented
        via an :class:`~easylink.implementation.Implementation` and thus do not
        require any sort of validation since no new data is created. This method
        only exists so that ``StandaloneSteps`` can be used interchangeably with other
        ``Steps`` in the codebase.

        Returns
        -------
            An empty dictionary.
        """
        return {}

    def set_configuration_state(
        self,
        step_config: LayeredConfigTree,
        combined_implementations: LayeredConfigTree,
        input_data_config: LayeredConfigTree,
    ) -> None:
        """Sets the configuration state to 'leaf'.

        Parameters
        ----------
        step_config
            The internal configuration of this ``Step``, i.e. it should not include
            the ``Step's`` name.
        combined_implementations
            The configuration for any ``Implementations`` to be combined.
        input_data_config
            The input data configuration for the entire pipeline.
        """
        self._configuration_state = LeafConfigurationState(
            self, step_config, combined_implementations, input_data_config
        )

    def add_edges_to_implementation_graph(self, implementation_graph):
        """Overwrites the super ``Step``'s method to do nothing.

        ``StandaloneSteps`` do not have edges within them in the ``ImplementationGraph``,
        since they are represented by a single ``NullImplementation`` node, and so we
        simply pass.
        """
        pass


class IOStep(StandaloneStep):
    """A type of :class:`StandaloneStep` used to represent incoming and outgoing data.

    ``IOSteps`` are used to handle the incoming and outgoing data to the pipeline;
    they are inherited by concrete :class:`InputStep` and :class:`OutputStep`
    classes.

    See :class:`Step` for inherited attributes.

    """

    def add_nodes_to_implementation_graph(
        self, implementation_graph: ImplementationGraph
    ) -> None:
        """Adds a :class:`~easylink.implementation.NullImplementation` node to the :class:`~easylink.graph_components.ImplementationGraph`."""
        implementation_graph.add_node_from_implementation(
            self.name,
            implementation=NullImplementation(
                self.name, self.input_slots.values(), self.output_slots.values()
            ),
        )


class InputStep(IOStep):
    """A special case type of :class:`IOStep` used to represent incoming data.

    An ``InputStep`` is used to pass data into the pipeline. Since we do not know
    what the data to pass into the pipeline will be a priori, we instantiate an
    "all" :class:`~easylink.graph_components.OutputSlot` which is used to pass in
    *all* data defined in the input data specification file.

    See :class:`IOStep` for inherited attributes.

    """

    def __init__(self) -> None:
        super().__init__(step_name="input_data", output_slots=(OutputSlot("all"),))

    def set_configuration_state(
        self,
        step_config: LayeredConfigTree,
        combined_implementations: LayeredConfigTree,
        input_data_config: LayeredConfigTree,
    ) -> None:
        """Sets the configuration state and updates the ``OutputSlots``.

        In addition to setting ``InputStep`` to a 'leaf' configuration state, this
        method also updates the ``OutputSlots`` to include all of the dataset keys
        in the input data specification file. This allows for future use of
        *specific* datasets instead of only *all* of them.

        Parameters
        ----------
        step_config
            The internal configuration of this ``Step``, i.e. it should not include
            the ``Step's`` name.
        combined_implementations
            The configuration for any implementations to be combined.
        input_data_config
            The input data configuration for the entire pipeline.
        """
        super().set_configuration_state(
            step_config, combined_implementations, input_data_config
        )
        for input_data_key in input_data_config:
            self.output_slots[input_data_key] = OutputSlot(name=input_data_key)


class OutputStep(IOStep):
    """A special case type of :class:`IOStep` used to represent final results data.

    An ``OutputStep`` is used to write the `Snakemake <https://snakemake.readthedocs.io/en/stable/>`_
    Snakefile target rule in the :meth:`easylink.pipeline.Pipeline.build_snakefile`
    method.

    See :class:`IOStep` for inherited attributes.

    """

    def __init__(self, input_slots: Iterable[InputSlot]) -> None:
        super().__init__("results", input_slots=input_slots)


class HierarchicalStep(Step):
    """A type of :class:`Step` that can may contain sub-``Steps``.

    A ``HierarchicalStep`` can be represented by multiple sub-``Steps`` (and thus
    implemented by the sub-``Steps'`` respective :class:`Implementations<easylink.implementation.Implementation>`.
    For example, "step_1" might be represented by a "step_1a" and a "step_1b", each
    of which has its own ``Implementation``.

    See :class:`Step` for inherited attributes.

    Parameters
    ----------
    nodes
        All sub-nodes (i.e. sub-``Steps``) that make up this ``HierarchicalStep``.
    edges
        The :class:`~easylink.graph_components.EdgeParams` of the sub-nodes.
    step_graph
        The :class:`~easylink.graph_components.StepGraph` i.e. the directed acyclic
        graph (DAG) of sub-nodes and their edges that make up this ``HierarchicalStep``.
    directly_implemented
        Whether or not the ``HierarchicalStep`` is implemented directly from the user.
        It is a convenience attribute to allow for back-end ``HierarchicalStep``
        construction (i.e. ones that do not have a corresponding user-provided
        'substeps' configuration key).

    """

    def __init__(
        self,
        step_name,
        name=None,
        input_slots=(),
        output_slots=(),
        nodes=(),
        edges=(),
        input_slot_mappings=(),
        output_slot_mappings=(),
        directly_implemented=True,
        default_implementation: str | None = None,
    ):
        super().__init__(
            step_name,
            name,
            input_slots,
            output_slots,
            input_slot_mappings,
            output_slot_mappings,
            default_implementation=default_implementation,
        )
        self.nodes = nodes
        """All sub-nodes (i.e. sub-``Steps``) that make up this ``HierarchicalStep``."""
        for node in self.nodes:
            node.set_parent_step(self)
        self.edges = edges
        """The :class:`~easylink.graph_components.EdgeParams` of the sub-nodes."""
        self.step_graph = self._get_step_graph(nodes, edges)
        """The :class:`~easylink.graph_components.StepGraph` i.e. the directed acyclic 
        graph (DAG) of sub-nodes and their edges that make up this ``HierarchicalStep``."""
        self.directly_implemented = directly_implemented
        """Whether or not the ``HierarchicalStep`` is user-configurable. It is a convenience
        attribute to allow for back-end ``HierarchicalStep`` creation that are not
        user-facing (i.e. they do not need to provide a 'substeps' configuration key)."""

        self._check_edges_are_valid()
        self._check_slot_mappings_are_valid()
        self._check_validators_are_consistent()

    @property
    def config_key(self):
        """The pipeline specification key required for a ``HierarchicalStep``."""
        return "substeps"

    def validate_step(
        self,
        step_config: LayeredConfigTree,
        combined_implementations: LayeredConfigTree,
        input_data_config: LayeredConfigTree,
    ) -> dict[str, list[str]]:
        """Validates the ``HierarchicalStep``.

        Parameters
        ----------
        step_config
            The internal configuration of this ``Step``, i.e. it should not include
            the ``Step's`` name.
        combined_implementations
            The configuration for any implementations to be combined.
        input_data_config
            The input data configuration for the entire pipeline.

        Returns
        -------
            A dictionary of errors, where the keys are the ``HierarchicalStep``
            name and the values are lists of error messages associated with the
            given ``HierarchicalStep``.

        Notes
        -----
        A ``HierarchicalStep`` can be in either a "leaf" or a "non-leaf" configuration
        state and the validation process is different for each.

        If the ``HierarchicalStep`` does not validate (i.e. errors are found and
        the returned dictionary is non-empty), the tool will exit and the pipeline
        will not run.

        We attempt to batch error messages as much as possible, but there may be
        times where the configuration is so ill-formed that we are unable to handle
        all issues in one pass. In these cases, new errors may be found after the
        initial ones are handled.
        """
        if self.directly_implemented:
            if self.config_key in step_config:
                step_config = step_config[self.config_key]
            else:
                # This is a leaf step
                return super().validate_step(
                    step_config, combined_implementations, input_data_config
                )
        return self._validate_step_graph(
            step_config, combined_implementations, input_data_config
        )

    def set_configuration_state(
        self,
        step_config: LayeredConfigTree,
        combined_implementations: LayeredConfigTree,
        input_data_config: LayeredConfigTree,
    ) -> None:
        """Sets the configuration state.

        The configuration state of a ``HierarchicalStep`` depends on (1) whether
        or not it is :attr:`directly_implemented` and (2) whether or not the
        :attr:`config_key` exists in the pipeline specification file.

        Parameters
        ----------
        step_config
            The internal configuration of this ``Step``, i.e. it should not include
            the ``Step's`` name.
        combined_implementations
            The configuration for any implementations to be combined.
        input_data_config
            The input data configuration for the entire pipeline.
        """
        if self.directly_implemented:
            if self.config_key in step_config:
                step_config = step_config[self.config_key]
                configuration_state_type = NonLeafConfigurationState
            else:
                configuration_state_type = LeafConfigurationState
        else:
            # Substeps must be used, so we require non-leaf here
            configuration_state_type = NonLeafConfigurationState
        self._configuration_state = configuration_state_type(
            self, step_config, combined_implementations, input_data_config
        )

    ##################
    # Helper methods #
    ##################

    def _get_step_graph(self, nodes: list[Step], edges: list[EdgeParams]) -> StepGraph:
        """Creates a :class:`~easylink.graph_components.StepGraph` from the nodes and edges the step was initialized with."""
        step_graph = StepGraph()
        for step in nodes:
            step_graph.add_node_from_step(step)
        for edge in edges:
            step_graph.add_edge_from_params(edge)
        return step_graph

    def _validate_step_graph(
        self,
        step_config: LayeredConfigTree,
        combined_implementations: LayeredConfigTree,
        input_data_config: LayeredConfigTree,
    ) -> dict[str, list[str]]:
        """Validates the nodes of a :class:`~easylink.graph_components.StepGraph`."""
        errors = {}
        for node in self.step_graph.nodes:
            step = self.step_graph.nodes[node]["step"]
            if isinstance(step, IOStep):
                continue
            if step.name not in step_config:
                default_implementation = self.step_graph.nodes[step.name][
                    "step"
                ].default_implementation
                step_errors = (
                    {f"step {step.name}": ["The step is not configured."]}
                    if not default_implementation
                    else {}
                )
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

    def _check_edges_are_valid(self):
        """Check that edges are valid, i.e. each connect two slots that actually exist."""
        for edge in self.edges:
            # Edges connect the *output* slot of a *source* node to the
            # *input* slot of a *target* node
            for slot_type, node_type in (("output", "source"), ("input", "target")):
                node_name = getattr(edge, f"{node_type}_node")
                if node_name not in self.step_graph.nodes:
                    raise ValueError(f"Edge {edge} has non-existent {node_type} node")
                if getattr(edge, f"{slot_type}_slot") not in getattr(
                    self.step_graph.nodes[node_name]["step"], f"{slot_type}_slots"
                ):
                    raise ValueError(f"Edge {edge} has non-existent {node_type} slot")

    def _check_slot_mappings_are_valid(self):
        """Check that input and output slot mappings are valid.

        Checks that the input and output slots on the parent step are all mapped,
        and that all slot mappings connect a slot on self (the parent) that actually exists
        to an slot that actually exists on a sub-step.
        """
        for slot_type in ["input", "output"]:
            slots = getattr(self, f"{slot_type}_slots")
            slot_mappings = self.slot_mappings[slot_type]

            if set(slots) != set(sm.parent_slot for sm in slot_mappings):
                raise ValueError(
                    f"{self.step_name} {slot_type} slots do not match {slot_type} slot mappings"
                )

            for sm in slot_mappings:
                if sm.child_node not in self.step_graph.nodes:
                    raise ValueError(
                        f"{self.step_name} {slot_type} slot {sm.parent_slot} maps to non-existent child node {sm.child_node}"
                    )
                if sm.child_slot not in getattr(
                    self.step_graph.nodes[sm.child_node]["step"], f"{slot_type}_slots"
                ):
                    raise ValueError(
                        f"{self.step_name} {slot_type} slot {sm.parent_slot} maps to non-existent slot {sm.child_slot} on child node {sm.child_node}"
                    )

    def _check_validators_are_consistent(self):
        """Check that if two input slots will receive the same data, they have the same validator.

        There are two versions of this to check: input slots that receive the same data because
        one is mapped to the other by a slot mapping, and input slots that receive the
        same data because they both are at the receiving end of edges from the same output slot.
        """
        # Check that input slots mapped to by our slot mappings have consistent validators
        for sm in self.slot_mappings["input"]:
            expected_validator = self.input_slots[sm.parent_slot].validator
            child_input_slot = self.step_graph.nodes[sm.child_node]["step"].input_slots[
                sm.child_slot
            ]
            if child_input_slot.validator != expected_validator:
                raise ValueError(
                    f"{sm.child_node}'s {sm.child_slot}, which is mapped from {self.step_name}'s {sm.parent_slot}, does not have the same validator"
                )

        # Check that input slots receiving the same data have consistent validators
        validators_by_child_output_slot = {}
        for edge in self.edges:
            child_input_slot = self.step_graph.edges[(edge.source_node, edge.target_node, 0)][
                "input_slot"
            ]
            source_slot = (edge.source_node, edge.output_slot)
            if source_slot not in validators_by_child_output_slot:
                validators_by_child_output_slot[source_slot] = child_input_slot.validator
            elif child_input_slot.validator != validators_by_child_output_slot[source_slot]:
                raise ValueError(
                    f"Not all input slots receiving edges from {edge.source_node}'s {edge.output_slot} have the same validator"
                )


class TemplatedStep(Step, ABC):
    """A type of :class:`Step` that may contain multiplicity.

    A ``TemplatedStep`` is used to represents a ``Step`` that contains a specified
    amount of multiplicity, such as one that is looped or run in parallel; it is
    inherited by concrete :class:`LoopStep` and :class:`CloneableStep` instances.

    See :class:`Step` for inherited attributes.

    Parameters
    ----------
    template_step
        The ``Step`` to be templated.

    """

    def __init__(
        self,
        template_step: Step,
        default_implementation: str | None = None,
    ) -> None:
        super().__init__(
            template_step.step_name,
            template_step.name,
            template_step.input_slots.values(),
            template_step.output_slots.values(),
            default_implementation=default_implementation,
        )
        self.step_graph = None
        """The :class:`~easylink.graph_components.StepGraph` i.e. the directed acyclic 
        graph (DAG) of sub-nodes and their edges that make up this ``TemplatedStep``."""
        self.template_step = template_step
        """The ``Step`` to be templated."""

        self.template_step.set_parent_step(self)

    @property
    @abstractmethod
    def node_prefix(self) -> str:
        """The prefix to be used in the node name.

        To disambiguate between the different types of nodes with multiplicity
        (i.e. loops or parallel), we use a unique prefix to be used as necessary.

        Returns
        -------
            The prefix to be used for the concrete ``TemplatedStep`` instances.
        """
        pass

    @abstractmethod
    def _update_step_graph(self, num_repeats: int) -> StepGraph:
        """Updates the :class:`~easylink.graph_components.StepGraph`.

        The ``TemplatedStep`` concrete instances must handle the fact that there
        is multiplicity in the ``StepGraph`` and update it accordingly.

        Parameters
        ----------
        num_repeats
            The number of copies to be made of the ``TemplatedStep``.

        Returns
        -------
            The updated ``StepGraph`` with unrolled ``Steps``.

        Notes
        -----
        We do not know a priori - or even during instantiation of the
        :class:`~easylink.pipeline_schema.PipelineSchema` - how many copies of any
        ``TemplatedSteps`` to make; indeed, there may be no ``TemplatedSteps`` at
        all. The user-provided pipeline configuration file must be read in in order
        to determine the number of multiples to generate.
        """
        pass

    @abstractmethod
    def _update_slot_mappings(self, num_repeats: int) -> dict[str, list[SlotMapping]]:
        """Updates the :class:`SlotMappings<easylink.graph_components.SlotMapping>`.

        Parameters
        ----------
        num_repeats
            The number of copies to be made of the ``TemplatedStep``.

        Returns
        -------
            Updated ``SlotMappings`` that account for the ``TemplatedStep`` multiplicity.
        """
        pass

    def validate_step(
        self,
        step_config: LayeredConfigTree,
        combined_implementations: LayeredConfigTree,
        input_data_config: LayeredConfigTree,
    ) -> dict[str, list[str]]:
        """Validates the ``TemplatedStep``.

        Regardless of whether or not a :attr:`Step.config_key` is set, we always
        validate the base ``Step`` used to create the ``TemplatedStep``. If a
        ``config_key`` is indeed set (that is, there is some multiplicity), we
        complete additional validations.

        Parameters
        ----------
        step_config
            The internal configuration of this ``Step``, i.e. it should not include
            the ``Step's`` name.
        combined_implementations
            The configuration for any implementations to be combined.
        input_data_config
            The input data configuration for the entire pipeline.

        Returns
        -------
            A dictionary of errors, where the keys are the ``TemplatedStep`` name
            and the values are lists of error messages associated with the given
            ``TemplatedStep``.

        Notes
        -----
        If the ``TemplatedStep`` does not validate (i.e. errors are found and the returned
        dictionary is non-empty), the tool will exit and the pipeline will not run.

        We attempt to batch error messages as much as possible, but there may be
        times where the configuration is so ill-formed that we are unable to handle
        all issues in one pass. In these cases, new errors may be found after the
        initial ones are handled.
        """
        if not self.config_key in step_config:
            # This is a leaf step
            return self.template_step.validate_step(
                step_config, combined_implementations, input_data_config
            )

        sub_config = step_config[self.config_key]

        if not isinstance(sub_config, list):
            return {
                f"step {self.name}": [
                    f"{self.node_prefix.capitalize()} instances must be formatted "
                    "as a sequence in the pipeline configuration."
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
                    LayeredConfigTree(parallel_config),
                    combined_implementations,
                    input_data_config,
                )
            )
            if parallel_errors:
                errors[f"step {self.name}"][f"{self.node_prefix}_{i+1}"] = parallel_errors
        return errors

    def set_configuration_state(
        self,
        step_config: LayeredConfigTree,
        combined_implementations: LayeredConfigTree,
        input_data_config: LayeredConfigTree,
    ):
        """Sets the configuration state to 'non-leaf'.

        In addition to setting the configuration state, this also updates the
        :class:`~easylink.graph_components.StepGraph` and
        :class:`SlotMappings<easylink.graph_components.SlotMapping>`.

        Parameters
        ----------
        step_config
            The internal configuration of this ``Step``, i.e. it should not include
            the ``Step's`` name.
        combined_implementations
            The configuration for any implementations to be combined.
        input_data_config
            The input data configuration for the entire pipeline.

        Notes
        -----
        A ``TemplatedStep`` is *always* assigned a :class:`NonLeafConfigurationState`
        even if it has no multiplicity since (despite having no copies to make) we
        still need to traverse the sub-``Steps`` to get to the one with a single
        :class:`~easylink.implementation.Implementation`, i.e. the one with a
        :class:`LeafConfigurationState`.
        """
        if self.config_key not in step_config:
            # Special handle the step_graph update
            self.step_graph = StepGraph()
            self.template_step.name = self.name
            self.step_graph.add_node_from_step(self.template_step)
            # Update the slot mappings with renamed children
            input_mappings = [
                InputSlotMapping(slot, self.template_step.name, slot)
                for slot in self.input_slots
            ]
            output_mappings = [
                OutputSlotMapping(slot, self.template_step.name, slot)
                for slot in self.output_slots
            ]
            self.slot_mappings = {"input": input_mappings, "output": output_mappings}
            # Add the key back to the expanded config
            expanded_config = LayeredConfigTree({self.template_step.name: step_config})
        else:
            expanded_config = self._get_config(step_config)
            num_repeats = len(expanded_config)
            self.step_graph = self._update_step_graph(num_repeats)
            self.slot_mappings = self._update_slot_mappings(num_repeats)

        # TemplatedSteps are by definition non-leaf steps.
        self._configuration_state = NonLeafConfigurationState(
            self, expanded_config, combined_implementations, input_data_config
        )

    ##################
    # Helper Methods #
    ##################

    def _get_config(self, step_config: LayeredConfigTree) -> LayeredConfigTree:
        """Convenience method to get the ``TemplatedStep's`` configuration.

        ``TemplatedSteps`` may include multiplicity. In such cases, their configurations
        must be modified to include the expanded ``Steps``.

        Parameters
        ----------
        step_config
            The high-level configuration of this ``TemplatedStep``.

        Returns
        -------
            The expanded sub-configuration of this ``TemplatedStep`` based on the
            :attr:`Step.config_key` and expanded to include all looped or parallelized
            sub-``Steps``).
        """
        if self.config_key in step_config:
            expanded_step_config = LayeredConfigTree()
            for i, sub_config in enumerate(step_config[self.config_key]):
                expanded_step_config.update(
                    {f"{self.name}_{self.node_prefix}_{i+1}": sub_config}
                )
            return expanded_step_config
        return step_config

    def _duplicate_template_step(self) -> Step:
        """Makes a duplicate of the template ``Step``.

        Returns
        -------
            A duplicate of the :attr:`template_step`.

        Notes
        -----
        A naive deepcopy would also make a copy of the :attr:`Step.parent_step`; we don't
        want this to be pointing to a *copy* of `self`, but rather to the original.
        We thus re-set the :attr:`Step.parent_step` to the original (`self`) after making
        the copy.
        """
        step_copy = copy.deepcopy(self.template_step)
        step_copy.set_parent_step(self)
        return step_copy


class LoopStep(TemplatedStep):
    """A type of :class:`TemplatedStep` that allows for looping.

    A ``LoopStep`` allows a user to loop a single :class:`Step` or a sequence
    of ``Steps`` multiple times such that each iteration depends on the previous.

    See :class:``TemplatedStep`` for inherited attributes.

    Parameters
    ----------
    template_step
        The ``Step`` to be templated.
    self_edges
        :class:`~easylink.graph_components.EdgeParams` that represent self-edges,
        i.e. edges that connect the output of one loop to the input of the next.

    """

    def __init__(
        self,
        template_step: Step | None = None,
        self_edges: Iterable[EdgeParams] = (),
        default_implementation: str | None = None,
    ) -> None:
        super().__init__(template_step, default_implementation)
        self.self_edges = self_edges
        """:class:`~easylink.graph_components.EdgeParams` that represent self-edges,
        i.e. edges that connect the output of one loop to the input of the next."""

    @property
    def config_key(self):
        """The pipeline specification key required for a ``LoopStep``."""
        return "iterations"

    @property
    def node_prefix(self):
        """The prefix to be used in the ``LoopStep`` node name."""
        return "loop"

    def _update_step_graph(self, num_repeats) -> StepGraph:
        """Updates the :class:`~easylink.graph_components.StepGraph` to include loops.

        This makes ``num_repeats`` copies of the :class:`TemplatedStep` and chains
        them together sequentially according to the self edges.

        Parameters
        ----------
        num_repeats
            The number of loops.

        Returns
        -------
            The updated ``StepGraph`` with ``num_repeats`` serial :class:`Steps<Step>`
            and their corrected edges.
        """
        graph = StepGraph()
        nodes = []
        edges = []

        for i in range(num_repeats):
            updated_step = self._duplicate_template_step()
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

    def _update_slot_mappings(self, num_repeats) -> dict[str, list[SlotMapping]]:
        """Updates the :class:`SlotMappings<easylink.graph_components.SlotMapping>`.

        This updates the appropriate slot mappings based on the number of loops
        and the non-self-edge input and output slots.

        Parameters
        ----------
        num_repeats
            The number of loops.

        Returns
        -------
            Updated ``SlotMappings`` that account for the number of loops requested.
        """
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


class CloneableStep(TemplatedStep):
    """A type of :class:`TemplatedStep` that creates multiple copies in parallel
    with no dependencies between them.

    See :class:`TemplatedStep` for inherited attributes.

    """

    @property
    def config_key(self):
        """The pipeline specification key required for a ``CloneableStep``."""
        return "clones"

    @property
    def node_prefix(self):
        """The prefix to be used in the ``CloneableStep`` node name."""
        return "clone"

    def _update_step_graph(self, num_repeats: int) -> StepGraph:
        """Updates the :class:`~easylink.graph_components.StepGraph` to include parallelization.

        This makes ``num_repeats`` copies of the ``TemplatedStep`` that are
        independent but contain the same edges.

        Parameters
        ----------
        num_repeats
            The number of parallel ``TemplatedSteps``.

        Returns
        -------
            The updated ``StepGraph`` with ``num_repeats`` parallel :class:`Steps<Step>`
            and their corrected edges.
        """
        graph = StepGraph()

        for i in range(num_repeats):
            updated_step = self._duplicate_template_step()
            updated_step.name = f"{self.name}_{self.node_prefix}_{i+1}"
            graph.add_node_from_step(updated_step)
        return graph

    def _update_slot_mappings(self, num_repeats: int) -> dict[str, list[SlotMapping]]:
        """Updates the :class:`SlotMappings<easylink.graph_components.SlotMapping>`.

        This updates the appropriate slot mappings based on the number of parallel
        copies and the existing input and output slots.

        Parameters
        ----------
        num_repeats
            The number of parallel copies.

        Returns
        -------
            Updated ``SlotMappings`` that account for the number of copies requested.
        """
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


class AutoParallelStep(Step):
    """A :class:`Step` that is run in parallel on the backend.

    An ``AutoParallelStep`` is different than a :class:`CloneableStep`
    in that it is not configured by the user to be run in parallel - it completely
    happens on the back end for performance reasons.

    See :class:`Step` for inherited attributes.

    Parameters
    ----------
    step
        The ``Step`` to be automatically run in parallel. To run multiple steps in
        parallel, use a :class:`HierarchicalStep`.
    slot_splitter_mapping
        A mapping of the :class:`~easylink.graph_components.InputSlot` name to split
        to the actual splitter function to be used.
    slot_aggregator_mapping
        A mapping of all :class:`~easylink.graph_components.OutputSlot` names to
        be aggregated and the actual aggregator function to be used.

    """

    def __init__(
        self,
        step: Step,
        slot_splitter_mapping: dict[str, Callable],
        slot_aggregator_mapping: dict[str, Callable],
    ) -> None:
        super().__init__(
            step_name=None,
            name=step.name,
            is_auto_parallel=True,
        )
        self.slot_splitter_mapping = slot_splitter_mapping
        """A mapping of the :class:`~easylink.graph_components.InputSlot` name to split
        to the actual splitter function to be used."""
        self.slot_aggregator_mapping = slot_aggregator_mapping
        """A mapping of all :class:`~easylink.graph_components.OutputSlot` names to
        be aggregated and the actual aggregator function to be used."""
        self.step_graph = None
        self.step = step
        self.step.set_parent_step(self)
        self.input_slots = self.step.input_slots
        self.output_slots = self.step.output_slots
        self._validate()
        # NOTE: We validated that the slot_splitter_mapping has only one item in self._validate()
        self.split_slot_name = list(self.slot_splitter_mapping.keys())[0]
        """The name of the ``InputSlot`` to be split."""

    @Step.name.setter
    def name(self, value: str) -> None:
        """Changes the name of the ``AutoParallelStep`` and the underlying :class:`Step` to the given value."""
        self._name = value
        self.step._name = value

    def _validate(self) -> None:
        """Validates the ``AutoParallelStep``.

        ``AutoParallelSteps`` are not configured by the user to be run
        in parallel. Since it happens on the back end, we need to do somewhat unique
        validations during construction. Specifically,
        - one and only one :class:`~easylink.graph_components.InputSlot` *must*
        be mapped to a splitter method.
        - all :class:`OutputSlots<easylink.graph_components.OutputSlot>` *must*
        be mapped to aggregator methods.
        """
        errors = []

        # check that only one input slot has a splitter assigned
        if len(self.slot_splitter_mapping) != 1:
            errors.append(
                f"AutoParallelStep '{self.step_name}' is attempting to define "
                f"{len(self.slot_splitter_mapping)} splitters when only one should be defined."
            )
        if len(self.slot_splitter_mapping) == 0:
            errors.append(
                f"AutoParallelStep '{self.step_name}' does not have any input slots with a "
                "splitter method assigned; one and only one input slot must have a splitter."
            )
        if len(self.slot_splitter_mapping) > 1:
            errors.append(
                f"AutoParallelStep '{self.step_name}' has multiple input slots with "
                "splitter methods assigned; one and only one input slot must have a splitter.\n"
                f"Input slots with splitters: {list(self.slot_splitter_mapping)}"
            )

        # check that all output slots have an aggregator assigned
        missing_aggregators = [
            slot.name
            for slot in self.output_slots.values()
            if slot.name not in self.slot_aggregator_mapping
        ]
        if len(missing_aggregators) != 0:
            errors.append(
                f"AutoParallelStep '{self.step_name}' has output slots without "
                f"aggregator methods assigned: {missing_aggregators}"
            )
        if errors:
            raise ValueError("\n".join(errors))

    def validate_step(
        self,
        step_config: LayeredConfigTree,
        combined_implementations: LayeredConfigTree,
        input_data_config: LayeredConfigTree,
    ) -> dict[str, list[str]]:
        """Validates the ``TemplatedStep``.

        Regardless of whether or not a :attr:`Step.config_key` is set, we always
        validate the base ``Step`` used to create the ``TemplatedStep``. If a
        ``config_key`` is indeed set (that is, there is some multiplicity), we
        complete additional validations.

        Parameters
        ----------
        step_config
            The internal configuration of this ``Step``, i.e. it should not include
            the ``Step's`` name.
        combined_implementations
            The configuration for any implementations to be combined.
        input_data_config
            The input data configuration for the entire pipeline.

        Returns
        -------
            A dictionary of errors, where the keys are the ``TemplatedStep`` name
            and the values are lists of error messages associated with the given
            ``TemplatedStep``.

        Notes
        -----
        If the ``TemplatedStep`` does not validate (i.e. errors are found and the returned
        dictionary is non-empty), the tool will exit and the pipeline will not run.

        We attempt to batch error messages as much as possible, but there may be
        times where the configuration is so ill-formed that we are unable to handle
        all issues in one pass. In these cases, new errors may be found after the
        initial ones are handled.
        """
        return self.step.validate_step(
            step_config, combined_implementations, input_data_config
        )

    def set_configuration_state(
        self,
        step_config: LayeredConfigTree,
        combined_implementations: LayeredConfigTree,
        input_data_config: LayeredConfigTree,
    ):
        """Sets the configuration state to 'non-leaf'.

        In addition to setting the configuration state, this also updates the
        :class:`~easylink.graph_components.StepGraph` and
        :class:`SlotMappings<easylink.graph_components.SlotMapping>`.

        Parameters
        ----------
        step_config
            The internal configuration of this ``Step``, i.e. it should not include
            the ``Step's`` name.
        combined_implementations
            The configuration for any implementations to be combined.
        input_data_config
            The input data configuration for the entire pipeline.
        """
        splitter_node_name = f"{self.name}_{self.split_slot_name}_split"
        splitter_step = SplitterStep(
            splitter_node_name,
            split_slot=self.input_slots[self.split_slot_name],
            splitter_func_name=self.slot_splitter_mapping[self.split_slot_name].__name__,
        )
        aggregator_node_name = f"{self.name}_aggregate"
        if len(self.output_slots) > 1:
            raise NotImplementedError(
                "FIXME [MIC-5883] Multiple output slots/files of AutoParallelSteps not yet supported"
            )
        output_slot = list(self.output_slots.values())[0]
        aggregator_step = AggregatorStep(
            aggregator_node_name,
            output_slot=output_slot,
            aggregator_func_name=self.slot_aggregator_mapping[output_slot.name].__name__,
            splitter_node_name=splitter_node_name,
        )
        self._update_step_graph(splitter_step, aggregator_step)
        self._update_slot_mappings(splitter_step, aggregator_step)
        # Add the key back to the expanded config
        expanded_config = LayeredConfigTree({self.step.name: step_config})
        # AutoParallelSteps are by definition non-leaf steps
        self._configuration_state = NonLeafConfigurationState(
            self, expanded_config, combined_implementations, input_data_config
        )

    def _update_step_graph(
        self, splitter_step: SplitterStep, aggregator_step: AggregatorStep
    ) -> StepGraph:
        """Updates the :class:`~easylink.graph_components.StepGraph` to include the splitting and aggregating nodes.

        This strings exactly three nodes together: the :class:`SplitterStep` that does
        the splitting of the input data, the actual :class:`Step` to be run in parallel,
        and the :class:`AggregatorStep` that aggregates the output data, i.e.
        ``SplitterStep -> ``Step`` -> AggregatorStep``.

        Notes
        -----
        The ``SplitterStep`` and ``AggregatorStep`` are backed by versions of
        :class:`NullImplementations<easylink.implementation.NullImplementation>`,
        i.e. they do *not* actually require containers to run.

        Parameters
        ----------
        splitter_step
            The :class:`SplitterStep` that does the splitting of the input data.
        aggregator_step
            The :class:`AggregatorStep` that aggregates the output data.

        Returns
        -------
            The updated ``StepGraph`` that includes ``SplitterStep``, ``Step``,
            and ``AggregatorStep`` nodes.
        """
        self.step_graph = StepGraph()
        for node in [splitter_step, self.step, aggregator_step]:
            self.step_graph.add_node_from_step(node)

        # Add SplitterStep -> Step edge
        self.step_graph.add_edge_from_params(
            EdgeParams(
                source_node=splitter_step.name,
                target_node=self.step.name,
                input_slot=self.split_slot_name,
                output_slot=list(splitter_step.output_slots.keys())[0],
            )
        )
        # Add the Step -> AggregatorStep edge
        if len(self.step.output_slots) > 1:
            raise NotImplementedError(
                "AutoParallelStep does not support multiple output slots."
            )
        self.step_graph.add_edge_from_params(
            EdgeParams(
                source_node=self.step.name,
                target_node=aggregator_step.name,
                input_slot=list(aggregator_step.input_slots.keys())[0],
                output_slot=list(self.step.output_slots.keys())[0],
            )
        )

    def _update_slot_mappings(
        self, splitter_step: SplitterStep, aggregator_step: AggregatorStep
    ) -> None:
        """Updates the :class:`SlotMappings<easylink.graph_components.SlotMapping>`.

        This updates the slot mappings to that the ``Step's`` inputs are redirected
        to the ``SplitterStep`` and the outputs are redirected to the ``AggregatorStep``.

        Parameters
        ----------
        splitter_step
            The :class:`SplitterStep` that does the splitting of the input data.
        aggregator_step
            The :class:`AggregatorStep` that aggregates the output data.

        Returns
        -------
            Updated ``SlotMappings`` that account for ``SplitterStep`` and ``AggregatorStep``.
        """
        # map the split input slot
        split_slot_name = list(splitter_step.input_slots.keys())[0]
        input_mappings = [
            InputSlotMapping(split_slot_name, splitter_step.name, split_slot_name)
        ]
        # map remaining input slots
        for input_slot in [slot for slot in self.input_slots if slot != split_slot_name]:
            input_mappings.append(InputSlotMapping(input_slot, self.step.name, input_slot))
        # map the output slots
        output_mappings = [
            OutputSlotMapping(slot, aggregator_step.name, slot) for slot in self.output_slots
        ]
        self.slot_mappings = {"input": input_mappings, "output": output_mappings}


class SplitterStep(StandaloneStep):
    """A :class:`StandaloneStep` that splits an :class:`~easylink.graph_components.InputSlot` for parallel processing.

    A ``SplitterStep`` is intended to be used in conjunction with a corresponding
    :class:`AggregatorStep` and only during construction of an :class:`AutoParallelStep`.

    See :class:`Step` for inherited attributes.

    Parameters
    ----------
    split_slot
        The name of the ``InputSlot`` to be split.
    splitter_func_name
        The name of the splitter function to be used.

    """

    def __init__(self, name: str, split_slot: InputSlot, splitter_func_name: str) -> None:
        # Remove the env_var (not an implemented step) and validator (will be validated
        # after the splitting during input to the actual step to run)
        input_slot = copy.deepcopy(split_slot)
        input_slot.env_var = None
        input_slot.validator = None
        super().__init__(
            name, input_slots=[input_slot], output_slots=[OutputSlot(f"{name}_main_output")]
        )
        self.splitter_func_name = splitter_func_name
        """The name of the splitter function to be used."""

    def add_nodes_to_implementation_graph(
        self, implementation_graph: ImplementationGraph
    ) -> None:
        """Adds a :class:`~easylink.implementation.NullImplementation` node to the :class:`~easylink.graph_components.ImplementationGraph`."""
        implementation_graph.add_node_from_implementation(
            self.name,
            implementation=NullSplitterImplementation(
                self.name,
                self.input_slots.values(),
                self.output_slots.values(),
                self.splitter_func_name,
            ),
        )


class AggregatorStep(StandaloneStep):
    def __init__(
        self,
        name: str,
        output_slot: OutputSlot,
        aggregator_func_name: str,
        splitter_node_name: str,
    ) -> None:
        """A :class:`StandaloneStep` that aggregates :class:`OutputSlots<easylink.graph_components.Outputslot>` after parallel processing.

        An ``AggregatorStep`` is intended to be used in conjunction with a corresponding
        :class:`SplitterStep` and only during construction of an :class:`AutoParallelStep`.

        See :class:`Step` for inherited attributes.

        Parameters
        ----------
        aggregator_func_name
            The name of the aggregator function to be used.
        splitter_node_name
            The name of the ``SplitterStep`` and its corresponding
            :class:`~easylink.implementation.NullSplitterImplementation` that this ``AggregatorStep``
            is associated with.
        """
        super().__init__(
            name,
            input_slots=[
                InputSlot(
                    f"{name}_main_input",
                    env_var=None,
                    validator=None,
                )
            ],
            output_slots=[output_slot],
        )
        self.aggregator_func_name = aggregator_func_name
        """The name of the aggregator function to be used."""
        self.splitter_node_name = splitter_node_name
        """The name of the ``SplitterStep`` and its corresponding
        :class:`~easylink.implementation.NullSplitterImplementation` that this ``AggregatorStep``
        is associated with."""

    def add_nodes_to_implementation_graph(
        self, implementation_graph: ImplementationGraph
    ) -> None:
        """Adds a :class:`~easylink.implementation.NullImplementation` node to the :class:`~easylink.graph_components.ImplementationGraph`."""
        implementation_graph.add_node_from_implementation(
            self.name,
            implementation=NullAggregatorImplementation(
                self.name,
                self.input_slots.values(),
                self.output_slots.values(),
                self.aggregator_func_name,
                self.splitter_node_name,
            ),
        )


class ChoiceStep(Step):
    """A type of :class:`Step` that allows for choosing from a set of options.

    See :class:`Step` for inherited attributes.

    Parameters
    ----------
    step_name
        The name of the ``ChoiceStep``.
    input_slots
        All required :class:`InputSlots<easylink.graph_components.InputSlot>`.
    output_slots
        All required :class:`OutputSlots<easylink.graph_components.OutputSlot>`.
    choices
        A dictionary of choices, where the keys are the names/types of choices and
        the values are dictionaries containing that type's ``Step`` and related
        :class:`SlotMappings<easylink.graph_components.SlotMapping>`.

    Notes
    -----
    ``ChoiceSteps`` are by definition non-leaf but do *not* require the typical
    :attr:`Step.config_key` in the pipeline specification file. Instead, the pipeline
    configuration must contain a 'type' key that specifies which option to choose.

    The :attr:`choices` dictionary must contain the choice type names as the outer
    keys. The values of each of these types is then another dictionary containing
    'step', 'input_slot_mappings', and 'output_slot_mappings' keys with their
    corresponding values.

    Each choice type must specify a *single* ``Step`` and its associated ``SlotMappings``.
    Any choice paths that require multiple sub-steps should specify a :class:`HierarchicalStep`.
    """

    def __init__(
        self,
        step_name: str,
        input_slots: Iterable[InputSlot],
        output_slots: Iterable[OutputSlot],
        choices: dict[str, dict[str, Step | SlotMapping]],
    ) -> None:
        super().__init__(
            step_name,
            input_slots=input_slots,
            output_slots=output_slots,
        )
        self.choices = choices
        """A dictionary of choices, where the keys are the names/types of choices and 
        the values are dictionaries containing that type's nodes, edges, and
        :class:`SlotMappings<easylink.graph_components.SlotMapping>`."""

    def validate_step(
        self,
        step_config: LayeredConfigTree,
        combined_implementations: LayeredConfigTree,
        input_data_config: LayeredConfigTree,
    ) -> dict[str, list[str]]:
        """Validates the ``ChoiceStep``.

        Parameters
        ----------
        step_config
            The internal configuration of this ``Step``, i.e. it should not include
            the ``Step's`` name.
        combined_implementations
            The configuration for any implementations to be combined.
        input_data_config
            The input data configuration for the entire pipeline.

        Returns
        -------
            A dictionary of errors, where the keys are the ``ChoiceStep`` name and the
            values are lists of error messages associated with the given ``Step``.

        Notes
        -----
        If the ``Step`` does not validate (i.e. errors are found and the returned
        dictionary is non-empty), the tool will exit and the pipeline will not run.

        We attempt to batch error messages as much as possible, but there may be
        times where the configuration is so ill-formed that we are unable to handle
        all issues in one pass. In these cases, new errors may be found after the
        initial ones are handled.

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
                    f"'{step_config.type}' is not a supported 'type'. Valid choices are: {list(self.choices)}."
                ]
            }

        chosen_step = self.choices[chosen_type]["step"]
        chosen_step_config = LayeredConfigTree(
            {key: value for key, value in step_config.items() if key != "type"}
        )
        if chosen_step.name not in chosen_step_config:
            return {
                f"step {self.name}": [
                    f"'{chosen_step.name}' is not configured. Confirm you have specified "
                    f"the correct steps for the '{chosen_type}' type."
                ]
            }
        # NOTE: A ChoiceStep is by definition non-leaf step
        return chosen_step.validate_step(
            chosen_step_config[chosen_step.name], combined_implementations, input_data_config
        )

    def set_configuration_state(
        self,
        step_config: LayeredConfigTree,
        combined_implementations: LayeredConfigTree,
        input_data_config: LayeredConfigTree,
    ):
        """Sets the configuration state to 'non-leaf'.

        In addition to setting the configuration state, this also updates the
        :class:`~easylink.graph_components.StepGraph` and
        :class:`SlotMappings<easylink.graph_components.SlotMapping>`.

        Parameters
        ----------
        step_config
            The internal configuration of this ``Step``, i.e. it should not include
            the ``Step's`` name.
        combined_implementations
            The configuration for any implementations to be combined.
        input_data_config
            The input data configuration for the entire pipeline.
        """
        choice = self.choices[step_config["type"]]
        self.step_graph = StepGraph()
        self.step_graph.add_node_from_step(choice["step"])
        self.slot_mappings = {
            "input": choice["input_slot_mappings"],
            "output": choice["output_slot_mappings"],
        }

        chosen_step_config = LayeredConfigTree(
            {key: value for key, value in step_config.items() if key != "type"}
        )
        # ChoiceSteps by definition are in a NonLeafConfigurationState
        self._configuration_state = NonLeafConfigurationState(
            self, chosen_step_config, combined_implementations, input_data_config
        )


class ConfigurationState(ABC):
    """A given :class:`Step's<Step>` configuration state.

    A ``ConfigurationState`` defines the exact pipeline configuration state for a
    given ``Step``, including the strategy required to get the :class:`~easylink.graph_components.ImplementationGraph`
    from it. There are two possible types of configuration states, "leaf" and "non-leaf",
    and each has its own concrete class, :class:`LeafConfigurationState` and
    ``NonLeafConfigurationState``, respectively.

    Parameters
    ----------
    step
        The ``Step`` this ``ConfigurationState`` is tied to.
    step_config
        The internal configuration of this ``Step`` we are setting the state
        for; it should not include the ``Step's`` name.
    combined_implementations
        The configuration for any implementations to be combined.
    input_data_config
        The input data configuration for the entire pipeline.

    """

    def __init__(
        self,
        step: Step,
        step_config: LayeredConfigTree,
        combined_implementations: LayeredConfigTree,
        input_data_config: LayeredConfigTree,
    ):
        self._step = step
        """The ``Step`` this ``ConfigurationState`` is tied to."""
        self.step_config = step_config
        """The internal configuration of this ``Step`` we are setting the state 
        for; it should not include the ``Step's`` name."""
        self.combined_implementations = combined_implementations
        """The relevant configuration if the ``Step's`` ``Implementation``
        has been requested to be combined with that of a different ``Step``."""
        self.input_data_config = input_data_config
        """The input data configuration for the entire pipeline."""

    @abstractmethod
    def add_nodes_to_implementation_graph(
        self, implementation_graph: ImplementationGraph
    ) -> None:
        """Adds this ``Step's`` ``Implementation(s)`` as nodes to the :class:`~easylink.graph_components.ImplementationGraph`."""
        pass

    @abstractmethod
    def add_edges_to_implementation_graph(
        self, implementation_graph: ImplementationGraph
    ) -> None:
        """Adds the edges of this ``Step's`` ``Implementation(s)`` to the :class:`~easylink.graph_components.ImplementationGraph`."""
        pass

    @abstractmethod
    def get_implementation_edges(self, edge: EdgeParams) -> list[EdgeParams]:
        """Gets the edge information for the ``Implementation`` related to this ``Step``.

        Parameters
        ----------
        edge
            The ``Step's`` edge information to be propagated to the ``ImplementationGraph``.

        Returns
        -------
            The ``Implementation's`` edge information.
        """
        pass


class LeafConfigurationState(ConfigurationState):
    """The :class:`ConfigurationState` for a leaf :class:`Step`.

    A ``LeafConfigurationState`` is a concrete class that corresponds to a leaf
    ``Step``, i.e. one that is implemented by a single :class:`~easylink.implementation.Implementation`.

    See :class:`ConfigurationState` for inherited attributes.

    """

    @property
    def is_combined(self) -> bool:
        """Whether or not this ``Step`` is combined with another ``Step``."""
        return COMBINED_IMPLEMENTATION_KEY in self.step_config

    @property
    def implementation_config(self) -> LayeredConfigTree:
        """The ``Step's`` specific ``Implementation`` configuration."""
        return (
            self.combined_implementations[self.step_config[COMBINED_IMPLEMENTATION_KEY]]
            if self.is_combined
            else self.step_config.implementation
        )

    def add_nodes_to_implementation_graph(
        self, implementation_graph: ImplementationGraph
    ) -> None:
        """Adds this ``Step's`` :class:`~easylink.implementation.Implementation` as a node to the :class:`~easylink.graph_components.ImplementationGraph`.

        A ``Step`` in a leaf configuration state by definition has no sub-``Steps``
        to unravel; we are able to directly instantiate an :class:`~easylink.implementation.Implementation`
        and add it to the ``ImplementationGraph``.
        """
        step = self._step
        if self.is_combined:
            if step.is_auto_parallel:
                raise NotImplementedError(
                    "Combining implementations with auto-parallel steps is not supported."
                )
            implementation = PartialImplementation(
                combined_name=self.step_config[COMBINED_IMPLEMENTATION_KEY],
                schema_step=step.step_name,
                input_slots=step.input_slots.values(),
                output_slots=step.output_slots.values(),
            )
        else:
            implementation = Implementation(
                schema_steps=[step.step_name],
                implementation_config=self.implementation_config,
                input_slots=step.input_slots.values(),
                output_slots=step.output_slots.values(),
                is_auto_parallel=step.is_auto_parallel,
            )
        implementation_graph.add_node_from_implementation(
            step.implementation_node_name,
            implementation=implementation,
        )

    def add_edges_to_implementation_graph(self, implementation_graph) -> None:
        """Adds the edges for this ``Step's`` ``Implementation`` to the ``ImplementationGraph``.

        ``Steps`` in a ``LeafConfigurationState`` do not actually have edges within them
        (they are represented by a single node in the ``ImplementationGraph``) and so
        we simply pass.
        """
        pass

    def get_implementation_edges(self, edge: EdgeParams) -> list[EdgeParams]:
        """Gets the edge information for the ``Implementation`` related to this ``Step``.

        Parameters
        ----------
        edge
            The ``Step's`` edge information to be propagated to the ``ImplementationGraph``.

        Raises
        ------
        ValueError
            If the ``Step`` is not in the edge or if no edges related to this ``Step`` are found.

        Returns
        -------
            The ``Implementation's`` edge information.
        """
        implementation_edges = []
        if edge.source_node == self._step.name:
            mappings = [
                mapping
                for mapping in self._step.get_implementation_slot_mappings()["output"]
                if mapping.parent_slot == edge.output_slot
            ]
            for mapping in mappings:
                imp_edge = mapping.remap_edge(edge)
                implementation_edges.append(imp_edge)
        elif edge.target_node == self._step.name:
            mappings = [
                mapping
                for mapping in self._step.get_implementation_slot_mappings()["input"]
                if mapping.parent_slot == edge.input_slot
            ]
            for mapping in mappings:
                # FIXME [MIC-5771]: Fix CloneableSteps
                if (
                    "input_data_file" in self.step_config
                    and edge.source_node == "pipeline_graph_input_data"
                ):
                    edge.output_slot = self.step_config["input_data_file"]
                imp_edge = mapping.remap_edge(edge)
                implementation_edges.append(imp_edge)
        else:
            raise ValueError(f"Step {self._step.name} not in edge {edge}")
        if not implementation_edges:
            raise ValueError(f"No edges found for Step {self._step.name} in edge {edge}")
        return implementation_edges


class NonLeafConfigurationState(ConfigurationState):
    """The :class:`ConfigurationState` for a non-leaf :class:`Step`.

    A ``NonLeafConfigurationState`` is a concrete class that corresponds to a non-leaf
    ``Step``, i.e. one that has a non-trivial :class:`~easylink.graph_components.StepGraph`.

    See :class:`ConfigurationState` for inherited attributes.

    Parameters
    ----------
    step
        The ``Step`` this ``ConfigurationState`` is tied to.
    step_config
        The internal configuration of this ``Step`` we are setting the state
        for; it should not include the ``Step's`` name (though it must include
        the sub-step names).
    combined_implementations
        The configuration for any :class:`Implementations<easylink.implementation.Implementation>`
        to be combined.
    input_data_config
        The input data configuration for the entire pipeline.

    Raises
    ------
    ValueError
        If the ``Step`` does not have a ``StepGraph``.

    Notes
    -----
    The first instance of a ``NonLeafConfigurationState`` is created when calling
    :meth:`~easylink.pipeline_schema.PipelineSchema.configure_pipeline` on the
    :class:`~easylink.pipeline_schema.PipelineSchema` that is chosen for a given
    EasyLink run; the ``step`` passed in is the entire ``PipelineSchema`` and the
    ``pipeline_config`` is that of the entire requested pipeline (which is by definition
    a non-leaf ``Step``).

    Upon instantiation of a ``NonLeafConfigurationState``, the
    :meth:`_configure_subgraph_steps` method is called which iterates through the
    ``Step's`` children and sets their configuration state. If any of these child
    ``Steps`` are also non-leaf, the process continues recursively until all
    nodes are leaf ``Steps`` with a corresponding :class:`LeafConfigurationState`.

    """

    def __init__(
        self,
        step: Step,
        step_config: LayeredConfigTree,
        combined_implementations: LayeredConfigTree,
        input_data_config: LayeredConfigTree,
    ):
        super().__init__(step, step_config, combined_implementations, input_data_config)
        if not step.step_graph:
            raise ValueError(
                "NonLeafConfigurationState requires a subgraph upon which to operate, "
                f"but Step {step.name} has no step graph."
            )
        self._configure_subgraph_steps()

    def add_nodes_to_implementation_graph(
        self, implementation_graph: ImplementationGraph
    ) -> None:
        """Adds this ``Step's`` ``Implementations`` as nodes to the ``ImplementationGraph``.

        This is a recursive function; it calls itself until all sub-``Steps``
        are of a ``LeafConfigurationState`` and have had their corresponding
        ``Implementations`` added as nodes to the ``ImplementationGraph``.
        """
        for node in self._step.step_graph.nodes:
            substep = self._step.step_graph.nodes[node]["step"]
            if self._step.is_auto_parallel:
                substep.is_auto_parallel = True
            substep.add_nodes_to_implementation_graph(implementation_graph)

    def add_edges_to_implementation_graph(
        self, implementation_graph: ImplementationGraph
    ) -> None:
        """Adds the edges of this ``Step's`` ``Implementations`` to the ``ImplementationGraph``.

        This method does two things:
        1. Adds the edges *at this level* (i.e. at the ``Step`` tied to this
        ``NonLeafConfigurationState``) to the ``ImplementationGraph``.
        2. Recursively traverses all sub-steps and adds their edges to the
        ``ImplementationGraph``.

        Note that to achieve (1), edges must be mapped from being between steps at
        this level of the hierarchy, all the way down to being between concrete implementations.
        Mapping each edge down to the implementation level is *itself* a recursive
        operation (see ``get_implementation_edges``).
        """
        # Add the edges at this level (i.e. the edges at this `self._step`)
        for source, target, edge_attrs in self._step.step_graph.edges(data=True):
            edge = EdgeParams.from_graph_edge(source, target, edge_attrs)
            source_step = self._step.step_graph.nodes[source]["step"]
            target_step = self._step.step_graph.nodes[target]["step"]

            source_edges = source_step.get_implementation_edges(edge)
            for source_edge in source_edges:
                for target_edge in target_step.get_implementation_edges(source_edge):
                    implementation_graph.add_edge_from_params(target_edge)

        # Recurse through all sub-steps and add the edges between them
        for node in self._step.step_graph.nodes:
            substep = self._step.step_graph.nodes[node]["step"]
            substep.add_edges_to_implementation_graph(implementation_graph)

    def get_implementation_edges(self, edge: EdgeParams) -> list[EdgeParams]:
        """Gets the edges for the ``Implementation`` related to this ``Step``.

        This method maps an edge between ``Steps`` in this ``Step's`` ``StepGraph``
        to one or more edges between ``Implementations`` by applying ``SlotMappings``.

        Parameters
        ----------
        edge
            The edge information of the edge in the ``StepGraph`` to be mapped to
            the ``Implementation`` level.

        Raises
        ------
        ValueError
            If the ``Step`` is not in the edge or if no edges related to this ``Step`` are found.

        Returns
        -------
            A list of edges between ``Implementations`` which are ready to add to
            the ``ImplementationGraph``.

        Notes
        -----
        In EasyLink, an edge (in either a ``StepGraph`` or ``ImplementationGraph``)
        sconnects two ``Slot``.

        The core of this method is to map the ``Slots`` on the ``StepGraph`` edge
        to the corresponding ``Slots`` on ``Implementations``.

        At each level in the step hierarchy, ``SlotMappings`` indicate how to map
        a ``Slot`` to the level below in the hierarchy.

        This method recurses through the step hierarchy until it reaches the leaf
        ``Steps`` relevant to this edge in order to compose all the ``SlotMappings``
        that should apply to it.

        Because a single ``Step`` can become multiple nodes in the ``ImplementationGraph``
        (e.g. a :class:`TemplatedStep`), a single edge between ``Steps`` may actually
        become multiple edges between ``Implementations``, which is why this method
        can return a list.
        """
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

    def _configure_subgraph_steps(self) -> None:
        """Sets the configuration state for all ``Steps`` in the ``StepGraph``.

        This method recursively traverses the ``StepGraph`` and sets the configuration
        state for each ``Step`` until reaching all leaf nodes.

        Notes
        -----
        If a ``Step`` name is missing from the ``step_config``, we know that it
        must have a default implementation because we already validated that one
        exists during :meth:`HierarchicalStep._validate_step_graph`. In that case,
        we manually instantiate and use a ``step_config`` with the default implementation.
        """
        for sub_node in self._step.step_graph.nodes:
            sub_step = self._step.step_graph.nodes[sub_node]["step"]
            try:
                step_config = (
                    self.step_config
                    if isinstance(sub_step, StandaloneStep)
                    else self.step_config[sub_step.name]
                )
            except KeyError:
                # We know that any missing keys must have a default implementation
                # (because we have already checked that it exists during validation)
                step_config = LayeredConfigTree(
                    {
                        "implementation": {
                            "name": sub_step.default_implementation,
                        }
                    }
                )
            sub_step.set_configuration_state(
                step_config, self.combined_implementations, self.input_data_config
            )
