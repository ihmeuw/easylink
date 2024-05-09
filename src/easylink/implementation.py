import os
from pathlib import Path
from typing import Dict, List, Optional

from easylink.configuration import Config
from easylink.step import Step
from easylink.utilities import paths
from easylink.utilities.data_utils import load_yaml


class Implementation:
    def __init__(
        self,
        config: Config,
        step: Step,
    ):
        self.step = step
        self.config = config
        self._pipeline_step_name = step.name
        self.name = config.get_implementation_name(step.name)
        self.environment_variables = config.pipeline["steps"][self.step.name][
            "implementation"
        ]["configuration"]
        self._metadata = self._load_metadata()
        self.step_name = self._metadata["step"]
        self.requires_spark = self._metadata.get("requires_spark", False)

    def __repr__(self) -> str:
        return f"Implementation.{self.step_name}.{self.name}"

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
        if self.step_name != self._pipeline_step_name:
            logs.append(
                f"Implementaton metadata step '{self.step_name}' does not "
                f"match pipeline configuration step '{self._pipeline_step_name}'"
            )
        return logs

    def _validate_container_exists(self, logs: List[Optional[str]]) -> List[Optional[str]]:
        err_str = f"Container '{self.singularity_image_path}' does not exist."
        if not Path(self.singularity_image_path).exists():
            logs.append(err_str)
        return logs

    @property
    def validation_filename(self) -> str:
        return self.name + "_validator"

    @property
    def singularity_image_path(self) -> str:
        return self._metadata["image_path"]

    @property
    def script_cmd(self) -> str:
        return self._metadata["script_cmd"]
