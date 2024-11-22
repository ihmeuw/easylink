from collections.abc import Iterable
from pathlib import Path

from layered_config_tree import LayeredConfigTree

from easylink.graph_components import InputSlot, OutputSlot
from easylink.utilities import paths
from easylink.utilities.data_utils import load_yaml


class Implementation:
    """A representation of an actual container that will be executed for a particular step.

    Implementations exist at a lower level than Steps. This class
    contains information about what container to use, what environment variables to set
    inside the container, and some metadata about the container.
    """

    def __init__(
        self,
        schema_steps: list[str],
        implementation_config: LayeredConfigTree,
        input_slots: Iterable["InputSlot"] = (),
        output_slots: Iterable["OutputSlot"] = (),
    ):
        self.name = implementation_config.name
        """The name of the implementation."""
        self.input_slots = {slot.name: slot for slot in input_slots}
        """A mapping of input slot names to InputSlot instances."""
        self.output_slots = {slot.name: slot for slot in output_slots}
        """A mapping of output slot names to OutputSlot instances."""
        self._metadata = self._load_metadata()
        self.environment_variables = self._get_env_vars(implementation_config)
        """A mapping of environment variables to set."""
        self.metadata_steps = self._metadata["steps"]
        """The specific step details that this implementation is associated with."""
        self.schema_steps = schema_steps
        """The high-level pipeline schema steps that this implementation is associated with."""
        self.requires_spark = self._metadata.get("requires_spark", False)
        """Whether this implementation requires a Spark environment."""

    def __repr__(self) -> str:
        return f"Implementation.{self.name}"

    def validate(self) -> list[str]:
        """Validates individual Implementation instances.

        This is intended to be run from the Pipeline validate method.
        """
        logs = []
        logs = self._validate_expected_step(logs)
        logs = self._validate_container_exists(logs)
        return logs

    ##################
    # Helper methods #
    ##################

    def _load_metadata(self) -> dict[str, str]:
        metadata = load_yaml(paths.IMPLEMENTATION_METADATA)
        return metadata[self.name]

    def _validate_expected_step(self, logs: list[str]) -> list[str]:
        if not set(self.schema_steps) == set(self.metadata_steps):
            logs.append(
                f"Pipeline configuration nodes {self.schema_steps} do not match metadata steps {self.metadata_steps}."
            )
        return logs

    def _validate_container_exists(self, logs: list[str]) -> list[str]:
        err_str = f"Container '{self.singularity_image_path}' does not exist."
        if not Path(self.singularity_image_path).exists():
            logs.append(err_str)
        return logs

    def _get_env_vars(self, implementation_config: LayeredConfigTree) -> dict[str, str]:
        env_vars = self._metadata.get("env", {})
        env_vars.update(implementation_config.get("configuration", {}))
        return env_vars

    @property
    def singularity_image_path(self) -> str:
        return self._metadata["image_path"]

    @property
    def script_cmd(self) -> str:
        return self._metadata["script_cmd"]

    @property
    def outputs(self) -> dict[str, list[str]]:
        return self._metadata["outputs"]


class NullImplementation:
    """A NullImplementation is used to represent a step that does not have an implementation.

    For example, the IO steps in the pipeline schema do not correspond to implementations
    but ImplementationGraph requires an "implementation" attribute with input and output slots
    for each node.
    """

    def __init__(
        self,
        name: str,
        input_slots: Iterable["InputSlot"] = (),
        output_slots: Iterable["OutputSlot"] = (),
    ):
        self.name = name
        self.input_slots = {slot.name: slot for slot in input_slots}
        self.output_slots = {slot.name: slot for slot in output_slots}
        self.schema_steps = [self.name]
        self.combined_name = None


class PartialImplementation:
    """A representation of one part of a combined implementation that spans multiple steps.

    A PartialImplementation is what is initially added to the implementation graph when
    a combined implementation is used (i.e. an implementation that spans multiple steps).
    We initially add a node for _each_ step, which has as its "implementation" attribute a
    PartialImplementation. Such a graph is not yet fit to run. When we make our second
    pass through, after the flat (non-hierarchical) PipelineGraph has been created, we find the set of
    PartialImplementation nodes corresponding to each combined implementation and
    replace them with a single node with a true Implementation representing the combined
    implementation.
    """

    def __init__(
        self,
        combined_name: str,
        schema_step: str,
        input_slots: Iterable["InputSlot"] = (),
        output_slots: Iterable["OutputSlot"] = (),
    ):
        self.combined_name = combined_name
        self.schema_step = schema_step
        self.input_slots = {slot.name: slot for slot in input_slots}
        self.output_slots = {slot.name: slot for slot in output_slots}
