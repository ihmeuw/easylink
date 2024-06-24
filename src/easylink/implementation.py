from pathlib import Path
from typing import Dict, List, Optional

from easylink.utilities import paths
from easylink.utilities.data_utils import load_yaml


class Implementation:
    """
    Implementations exist at a lower level than Steps. They are representations of the
    actual containers that will be executed for a particular step in the pipeline. This class
    contains information about what container to use, what environment variables to set
    inside the container, and some metadata about the container.
    """

    def __init__(self, name: str, step_name: str, environment_variables: Dict[str, str]):
        self.name = name
        self.environment_variables = environment_variables
        self._metadata = self._load_metadata()
        self.metadata_step_name = self._metadata["step"]
        self.schema_step_name = step_name
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
        if self.metadata_step_name != self.schema_step_name:
            logs.append(
                f"Implementaton metadata step '{self.metadata_step_name}' does not "
                f"match pipeline configuration step '{self.schema_step_name}'"
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
