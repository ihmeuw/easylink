from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Dict, Iterable, List, Optional, Sequence

from layered_config_tree import LayeredConfigTree

from easylink.utilities import paths
from easylink.utilities.data_utils import load_yaml

if TYPE_CHECKING:
    from easylink.graph_components import InputSlot, OutputSlot


class Implementation:
    """
    Implementations exist at a lower level than Steps. They are representations of the
    actual containers that will be executed for a particular step in the pipeline. This class
    contains information about what container to use, what environment variables to set
    inside the container, and some metadata about the container.
    """

    def __init__(
        self,
        schema_steps: Sequence[str],
        implementation_config: LayeredConfigTree,
        input_slots: Iterable["InputSlot"] = (),
        output_slots: Iterable["OutputSlot"] = (),
    ):
        self.name = implementation_config.name
        self.input_slots = {slot.name: slot for slot in input_slots}
        self.output_slots = {slot.name: slot for slot in output_slots}
        self.environment_variables = implementation_config.to_dict().get("configuration", {})
        self._metadata = self._load_metadata()
        self.metadata_steps = set(self._metadata["steps"])
        self.is_joint = len(self.metadata_steps) > 1
        self.schema_steps = set(schema_steps)
        self.requires_spark = self._metadata.get("requires_spark", False)

    def __repr__(self) -> str:
        return f"Implementation.{self.step_name}.{self.name}"

    @classmethod
    def merge_implementations(
        cls, implementations: Sequence[Implementation]
    ) -> Implementation:
        # Raise if the implementations have different names
        if len(set(impl.name for impl in implementations)) > 1:
            raise ValueError("Implementations must have the same name to be merged.")
        implementation_name = implementations[0].name
        schema_steps = {
            step for implementation in implementations for step in implementation.schema_steps
        }
        input_slots = [
            slot
            for implementation in implementations
            for slot in implementation.input_slots.values()
        ]
        output_slots = [
            slot
            for implementation in implementations
            for slot in implementation.output_slots.values()
        ]
        implementation_config = LayeredConfigTree()
        implementation_config.update({"name": implementation_name})
        implementation_config.update(
            {
                "configuration": {
                    env_var: value
                    for implementation in implementations
                    for env_var, value in implementation.environment_variables.items()
                }
            }
        )

        return Implementation(schema_steps, implementation_config, input_slots, output_slots)

    def validate(self) -> List[Optional[str]]:
        """Validates individual Implementation instances. This is intended to be
        run from the Pipeline validate method.
        """
        logs = []
        logs = self._validate_expected_step(logs)
        logs = self._validate_container_exists(logs)
        return logs

    ##################
    # Helper methods #
    ##################

    def _load_metadata(self) -> Dict[str, str]:
        metadata = load_yaml(paths.IMPLEMENTATION_METADATA)
        return metadata[self.name]

    def _validate_expected_step(self, logs: List[Optional[str]]) -> List[Optional[str]]:
        if not self.schema_steps.issubset(self.metadata_steps):
            logs.append(
                f"Implementaton metadata steps '{self.metadata_steps}' does not "
                f"match pipeline configuration step '{self.schema_steps}'"
            )
        return logs

    def _validate_container_exists(self, logs: List[Optional[str]]) -> List[Optional[str]]:
        err_str = f"Container '{self.singularity_image_path}' does not exist."
        if not Path(self.singularity_image_path).exists():
            logs.append(err_str)
        return logs

    @property
    def singularity_image_path(self) -> str:
        return self._metadata["image_path"]

    @property
    def script_cmd(self) -> str:
        return self._metadata["script_cmd"]

    @property
    def outputs(self) -> Dict[str, List[str]]:
        return self._metadata["outputs"]


class NullImplementation:
    """A NullImplementation is used to represent a step that does not have an implementation.
    For example, the IO steps in the pipeline schema do not correspond to implementations
    but ImplementationGraph requires an "implementation" attribute with input and output slots
    for each node."""

    def __init__(
        self,
        name: str,
        input_slots: Iterable["InputSlot"] = (),
        output_slots: Iterable["OutputSlot"] = (),
    ):
        self.name = name
        self.input_slots = {slot.name: slot for slot in input_slots}
        self.output_slots = {slot.name: slot for slot in output_slots}
        self.is_joint = False
