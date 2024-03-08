import os
from pathlib import Path
from typing import Dict, List, Optional

from linker.configuration import Config
from linker.step import Step
from linker.utilities import paths
from linker.utilities.data_utils import load_yaml


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
        ].get("configuration", {})
        self._metadata = self._load_metadata()
        self.step_name = self._metadata[self.name]["step"]
        self._requires_spark = self._metadata[self.name].get("requires_spark", False)
        self._container_full_stem = self._get_container_full_stem()

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
        return metadata

    def _get_container_full_stem(self) -> str:
        return f"{self._metadata[self.name]['container_path']}/{self._metadata[self.name]['name']}"

    def _get_script_full_stem(self) -> str:
        return f"{self._metadata[self.name]['script_cmd']}"

    def _validate_expected_step(self, logs: List[Optional[str]]) -> List[Optional[str]]:
        if self.step_name != self._pipeline_step_name:
            logs.append(
                f"Implementaton metadata step '{self.step_name}' does not "
                f"match pipeline configuration step '{self._pipeline_step_name}'"
            )
        return logs

    def _validate_container_exists(self, logs: List[Optional[str]]) -> List[Optional[str]]:
        err_str = f"Container '{self._container_full_stem}' does not exist."
        if (
            self.config.container_engine == "docker"
            and not Path(f"{self._container_full_stem}.tar.gz").exists()
        ):
            logs.append(err_str)
        if (
            self.config.container_engine == "singularity"
            and not Path(f"{self._container_full_stem}.sif").exists()
        ):
            logs.append(err_str)
        if (
            self.config.container_engine == "undefined"
            and not Path(f"{self._container_full_stem}.tar.gz").exists()
            and not Path(f"{self._container_full_stem}.sif").exists()
        ):
            logs.append(err_str)
        return logs

    @property
    def validation_filename(self):
        return self.name + "_validator"

    @property
    def singularity_image_path(self):
        return self._get_container_full_stem + ".sif"

    @property
    def script_cmd(self):
        return self._get_script_full_stem
