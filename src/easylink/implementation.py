"""
===============
Implementations
===============

This module is responsible for defining the abstractions that represent actual
implementations of steps in a pipeline. Typically, these abstractions contain
information about what container to run for a given step and other related details.

"""

from collections.abc import Iterable
from pathlib import Path

from layered_config_tree import LayeredConfigTree

from easylink.graph_components import InputSlot, OutputSlot
from easylink.utilities import paths
from easylink.utilities.data_utils import load_yaml


class Implementation:
    """A representation of an actual container that will be executed for a :class:`~easylink.step.Step`.

    ``Implementations`` exist at a lower level than ``Steps``. This class contains
    information about what container to use, what environment variables to set
    inside the container, and some metadata about the container.

    Parameters
    ----------
    schema_steps
        The user-requested ``Step`` names for which this ``Implementation`` is
        expected to implement.
    implementation_config
        The configuration details required to run the relevant container.
    input_slots
        All required :class:`InputSlots<easylink.graph_components.InputSlot>`.
    output_slots
        All required :class:`OutputSlots<easylink.graph_components.OutputSlot>`.
    """

    def __init__(
        self,
        schema_steps: list[str],
        implementation_config: LayeredConfigTree,
        input_slots: Iterable["InputSlot"] = (),
        output_slots: Iterable["OutputSlot"] = (),
        is_embarrassingly_parallel: bool = False,
    ):
        self.name = implementation_config.name
        """The name of this ``Implementation``."""
        self.input_slots = {slot.name: slot for slot in input_slots}
        """A mapping of ``InputSlot`` names to their instances."""
        self.output_slots = {slot.name: slot for slot in output_slots}
        """A mapping of ``OutputSlot`` names to their instances."""
        self._metadata = self._load_metadata()
        self.environment_variables = self._get_env_vars(implementation_config)
        """A mapping of environment variables to set."""
        self.metadata_steps = self._metadata["steps"]
        """The names of the specific ``Steps`` for which this ``Implementation`` 
        has been designed to implement."""
        self.schema_steps = schema_steps
        """The names of the specific ``Steps`` that the user has requested to be
        implemented by this particular ``Implementation``."""
        self.requires_spark = self._metadata.get("requires_spark", False)
        """Whether this ``Implementation`` requires a Spark environment."""
        self.is_embarrassingly_parallel = is_embarrassingly_parallel

    def __repr__(self) -> str:
        return f"Implementation.{self.name}"

    def validate(self) -> list[str]:
        """Validates individual ``Implementation`` instances.

        Returns
        -------
            A list of logs containing any validation errors. Each item in the list
            is a distinct message about a particular validation error (e.g. if a
            required container does not exist).

        Notes
        -----
        This is intended to be run from :meth:`easylink.pipeline.Pipeline._validate`.
        """
        logs = []
        logs = self._validate_expected_steps(logs)
        logs = self._validate_container_exists(logs)
        return logs

    ##################
    # Helper methods #
    ##################

    def _load_metadata(self) -> dict[str, str]:
        """Loads the relevant implementation metadata."""
        metadata = load_yaml(paths.IMPLEMENTATION_METADATA)
        return metadata[self.name]

    def _validate_expected_steps(self, logs: list[str]) -> list[str]:
        """Validates that the ``Implementation`` is responsible for the correct steps."""
        if not set(self.schema_steps) == set(self.metadata_steps):
            logs.append(
                f"Pipeline configuration nodes {self.schema_steps} do not match "
                f"metadata steps {self.metadata_steps}."
            )
        return logs

    def _validate_container_exists(self, logs: list[str]) -> list[str]:
        """Validates that the container to run exists."""
        err_str = f"Container '{self.singularity_image_path}' does not exist."
        if not Path(self.singularity_image_path).exists():
            logs.append(err_str)
        return logs

    def _get_env_vars(self, implementation_config: LayeredConfigTree) -> dict[str, str]:
        """Gets the relevant environment variables."""
        env_vars = self._metadata.get("env", {})
        env_vars.update(implementation_config.get("configuration", {}))
        return env_vars

    @property
    def singularity_image_path(self) -> str:
        """The path to the required Singularity image."""
        return self._metadata["image_path"]

    @property
    def script_cmd(self) -> str:
        """The command to run inside of the container."""
        return self._metadata["script_cmd"]

    @property
    def outputs(self) -> dict[str, list[str]]:
        """The expected output metadata."""
        return self._metadata["outputs"]


class NullImplementation:
    """A partial :class:`Implementation` interface when no container is needed to run.

    The primary use case for this class is when adding an
    :class:`~easylink.step.IOStep` - which does not have a corresponding
    ``Implementation`` - to an :class:`~easylink.graph_components.ImplementationGraph`
    since adding any new node requires an object with :class:`~easylink.graph_components.InputSlot`
    and :class:`~easylink.graph_components.OutputSlot` names.

    Parameters
    ----------
    name
        The name of this ``NullImplementation``.
    input_slots
        All required ``InputSlots``.
    output_slots
        All required ``OutputSlots``.
    """

    def __init__(
        self,
        name: str,
        input_slots: Iterable["InputSlot"] = (),
        output_slots: Iterable["OutputSlot"] = (),
    ):
        self.name = name
        """The name of this ``NullImplementation``."""
        self.input_slots = {slot.name: slot for slot in input_slots}
        """A mapping of ``InputSlot`` names to their instances."""
        self.output_slots = {slot.name: slot for slot in output_slots}
        """A mapping of ``OutputSlot`` names to their instances."""
        self.schema_steps = [self.name]
        """The requested :class:`~easylink.step.Step` names this ``NullImplementation`` implements."""
        self.combined_name = None
        """The name of the combined implementation of which ``NullImplementation`` 
        is a constituent. This is definitionally None."""


class PartialImplementation:
    """One part of a combined implementation that spans multiple :class:`Steps<easylink.step.Step>`.

    A ``PartialImplementation`` is what is initially added to the
    :class:`~easylink.graph_components.ImplementationGraph` when a so-called
    "combined implementation" is used (i.e. an :class:`Implementation` that spans
    multiple ``Steps``). We initially add a node for *each* ``Step``, which has as
    its ``implementation`` attribute a ``PartialImplementation``. Such a graph is not
    yet fit to run. When we make our second pass through, after the flat (non-hierarchical)
    :class:`~easylink.pipeline_graph.PipelineGraph` has been created, we find the
    set of ``PartialImplementation`` nodes corresponding to each combined implementation
    and replace them with a single node with a true ``Implementation`` representing
    the combined implementation.

    Parameters
    ----------
    combined_name
        The name of the combined implementation of which this ``PartialImplementation``
        is a part.
    schema_step
        The requested ``Step`` name that this ``PartialImplementation`` partially
        implements.
    input_slots
        The :class:`InputSlots<easylink.graph_components.InputSlot>` for this ``PartialImplementation``.
    output_slots
        The :class:`OutputSlots<easylink.graph_components.OutputSlot>` for this ``PartialImplementation``.

    """

    def __init__(
        self,
        combined_name: str,
        schema_step: str,
        input_slots: Iterable["InputSlot"] = (),
        output_slots: Iterable["OutputSlot"] = (),
    ):
        self.combined_name = combined_name
        """The name of the combined implementation of which this ``PartialImplementation``
        is a part."""
        self.schema_step = schema_step
        """The requested ``Step`` name that this ``PartialImplementation`` partially 
        implements."""
        self.input_slots = {slot.name: slot for slot in input_slots}
        """A mapping of ``InputSlot`` names to their instances."""
        self.output_slots = {slot.name: slot for slot in output_slots}
        """A mapping of ``OutputSlot`` names to their instances."""
