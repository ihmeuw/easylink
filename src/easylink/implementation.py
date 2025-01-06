"""
===============
Implementations
===============

This module is responsible for defining the abstractions that represent actual
implementations of steps in a pipeline.

"""

from collections.abc import Iterable
from pathlib import Path

from layered_config_tree import LayeredConfigTree

from easylink.graph_components import InputSlot, OutputSlot
from easylink.utilities import paths
from easylink.utilities.data_utils import load_yaml


class Implementation:
    """A representation of an actual container that will be executed for a :class:`~easylink.step.Step`.

    Implementations exist at a lower level than :class:`Steps<easylink.step.Step>`.
    This class contains information about what container to use, what environment
    variables to set inside the container, and some metadata about the container.

    Parameters
    ----------
    schema_steps
        The requested :class:`~easylink.pipeline_schema.PipelineSchema`
        :class:`~easylink.step.Step` names for which this Implementation is
        expected to be responsible.
    implementation_config
        The configuration for this Implementation.
    input_slots
        The :class:`InputSlots<easylink.graph_components.InputSlot>` for this Implementation.
    output_slots
        The :class:`OutputSlots<easylink.graph_components.OutputSlot>` for this Implementation.
    """

    def __init__(
        self,
        schema_steps: list[str],
        implementation_config: LayeredConfigTree,
        input_slots: Iterable["InputSlot"] = (),
        output_slots: Iterable["OutputSlot"] = (),
    ):
        self.name = implementation_config.name
        """The name of this Implementation."""
        self.input_slots = {slot.name: slot for slot in input_slots}
        """A mapping of :class:`InputSlots<easylink.graph_components.InputSlot>`
        names to their instances."""
        self.output_slots = {slot.name: slot for slot in output_slots}
        """A mapping of :class:`OutputSlots<easylink.graph_components.OutputSlot>` 
        names to their instances."""
        self._metadata = self._load_metadata()
        self.environment_variables = self._get_env_vars(implementation_config)
        """A mapping of environment variables to set."""
        self.metadata_steps = self._metadata["steps"]
        """The names of the specific :class:`Steps<easylink.step.Step>` for which
        this Implementation is responsible."""
        self.schema_steps = schema_steps
        """The requested :class:`~easylink.pipeline_schema.PipelineSchema`
        :class:`~easylink.step.Step` names for which this Implementation is 
        requested to be responsible in the pipeline."""
        self.requires_spark = self._metadata.get("requires_spark", False)
        """Whether this Implementation requires a Spark environment."""

    def __repr__(self) -> str:
        return f"Implementation.{self.name}"

    def validate(self) -> list[str]:
        """Validates individual Implementation instances.

        Returns
        -------
            A list of logs containing any validation errors.

        Notes
        -----
        This is intended to be run from :meth:`easylink.pipeline.Pipeline._validate`.
        """
        logs = []
        logs = self._validate_expected_step(logs)
        logs = self._validate_container_exists(logs)
        return logs

    ##################
    # Helper methods #
    ##################

    def _load_metadata(self) -> dict[str, str]:
        """Loads the metadata for this Implementation instance."""
        metadata = load_yaml(paths.IMPLEMENTATION_METADATA)
        return metadata[self.name]

    def _validate_expected_step(self, logs: list[str]) -> list[str]:
        """Validates that the Implementation is responsible for the correct steps."""
        if not set(self.schema_steps) == set(self.metadata_steps):
            logs.append(
                f"Pipeline configuration nodes {self.schema_steps} do not match metadata steps {self.metadata_steps}."
            )
        return logs

    def _validate_container_exists(self, logs: list[str]) -> list[str]:
        """Validates that the container for this Implementation exists."""
        err_str = f"Container '{self.singularity_image_path}' does not exist."
        if not Path(self.singularity_image_path).exists():
            logs.append(err_str)
        return logs

    def _get_env_vars(self, implementation_config: LayeredConfigTree) -> dict[str, str]:
        """Gets the environment variables relevant to this Implementation."""
        env_vars = self._metadata.get("env", {})
        env_vars.update(implementation_config.get("configuration", {}))
        return env_vars

    @property
    def singularity_image_path(self) -> str:
        """The path to the Singularity image for this Implementation."""
        return self._metadata["image_path"]

    @property
    def script_cmd(self) -> str:
        """The command to run inside of the container for this Implementation."""
        return self._metadata["script_cmd"]

    @property
    def outputs(self) -> dict[str, list[str]]:
        """The outputs expected from this Implementation."""
        return self._metadata["outputs"]


class NullImplementation:
    """A representation of a :class:`~easylink.step.Step` that does not have an :class:`Implementation`.

    Parameters
    ----------
    name
        The name of this NullImplementation.
    input_slots
        The :class:`InputSlots<easylink.graph_components.InputSlot>` for this NullImplementation.
    output_slots
        The :class:`OutputSlots<easylink.graph_components.OutputSlot>` for this NullImplementation.

    Notes
    -----
    The primary use case for this class is when adding an :class:`~easylink.step.IOStep` -
    which does not have a corresponding :class:`Implementation` - to an
    :class:`~easylink.graph_components.ImplementationGraph`. Adding a new node
    requires an ``implementation`` attribute with :class:`~easylink.graph_components.InputSlot`
    and :class:`~easylink.graph_components.OutputSlot` names.

    """

    def __init__(
        self,
        name: str,
        input_slots: Iterable["InputSlot"] = (),
        output_slots: Iterable["OutputSlot"] = (),
    ):
        self.name = name
        """The name of this NullImplementation."""
        self.input_slots = {slot.name: slot for slot in input_slots}
        """A mapping of :class:`InputSlots<easylink.graph_components.InputSlot>`
        names to their instances."""
        self.output_slots = {slot.name: slot for slot in output_slots}
        """A mapping of :class:`OutputSlots<easylink.graph_components.OutputSlot>`
        names to their instances."""
        self.schema_steps = [self.name]
        """The requested :class:`~easylink.pipeline_schema.PipelineSchema`
        :class:`~easylink.step.Step` names for which this NullImplementation is
        expected to be responsible."""
        self.combined_name = None
        """The name of the combined implementation that this NullImplementation 
        is part of. This is definitionally None for a NullImplementation."""


class PartialImplementation:
    """A representation of one part of a combined implementation that spans multiple :class:`Steps<easylink.step.Step>`.

    A PartialImplementation is what is initially added to the :class:`~easylink.graph_components.ImplementationGraph`
    when a so-called "combined implementation" is used (i.e. an :class:`Implementation`
    that spans multiple :class:`Steps<easylink.step.Step>`).
    We initially add a node for _each_ :class:`~easylink.step.Step`, which has as
    its ``implementation`` attribute a PartialImplementation. Such a graph is not
    yet fit to run. When we make our second pass through, after the flat (non-hierarchical)
    :class:`~easylink.pipeline_graph.PipelineGraph` has been created, we find the
    set of PartialImplementation nodes corresponding to each combined implementation
    and replace them with a single node with a true :class:`Implementation` representing
    the combined implementation.

    Parameters
    ----------
    combined_name
        The name of the combined implementation that this PartialImplementation
        is part of.
    schema_step
        The requested :class:`~easylink.pipeline_schema.PipelineSchema`
        :class:`~easylink.step.Step` name for which this PartialImplementation is
        expected to be responsible.
    input_slots
        The :class:`InputSlots<easylink.graph_components.InputSlot>` for this PartialImplementation.
    output_slots
        The :class:`OutputSlots<easylink.graph_components.OutputSlot>` for this PartialImplementation.

    """

    def __init__(
        self,
        combined_name: str,
        schema_step: str,
        input_slots: Iterable["InputSlot"] = (),
        output_slots: Iterable["OutputSlot"] = (),
    ):
        self.combined_name = combined_name
        """The name of the combined implementation that this PartialImplementation
        is part of."""
        self.schema_step = schema_step
        """The requested :class:`~easylink.pipeline_schema.PipelineSchema`
        :class:`~easylink.step.Step` name for which this PartialImplementation is
        expected to be responsible."""
        self.input_slots = {slot.name: slot for slot in input_slots}
        """A mapping of :class:`InputSlots<easylink.graph_components.InputSlot>`
        names to their instances."""
        self.output_slots = {slot.name: slot for slot in output_slots}
        """A mapping of :class:`OutputSlots<easylink.graph_components.OutputSlot>`
        names to their instances."""
