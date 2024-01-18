from pathlib import Path
from typing import Dict, List, Optional, Union

from linker.utilities.data_utils import input_file_validator, load_yaml


class Config:
    """A container for configuration information where each value is exposed
    as an attribute.

    """

    def __init__(
        self,
        pipeline_specification: Optional[Path],
        input_data: Optional[Path],
        computing_environment: Optional[str],
    ):
        self.pipeline_path = pipeline_specification
        self.pipeline = load_yaml(self.pipeline_path) if pipeline_specification else {}
        self.input_data = self._load_input_data_paths(input_data) if input_data else []
        self.computing_environment_path = (
            Path(computing_environment) if computing_environment else None
        )
        self.environment = self._load_computing_environment(self.computing_environment_path)

        self.computing_environment = self.environment["computing_environment"]
        self.container_engine = self.environment.get("container_engine", "undefined")
        self._validate()

    def get_resources(self) -> Dict[str, str]:
        return {
            **self.environment["implementation_resources"],
            **self.environment[self.environment["computing_environment"]],
        }

    def get_spark_resources(self) -> Dict[str, str]:
        return {**self.environment["spark"]}

    #################
    # Setup Methods #
    #################

    def _validate(self) -> None:
        # TODO [MIC-4723]: validate configuration files

        if not self.container_engine in ["docker", "singularity", "undefined"]:
            raise NotImplementedError(
                f"Container engine '{self.container_engine}' is not supported."
            )

        # Validate input data against constraints
        for input_file in self.input_data:
            input_file_validator(input_file)

    @staticmethod
    def _load_computing_environment(
        computing_environment_path: Optional[Path],
    ) -> Dict[str, Union[Dict, str]]:
        """Load the computing environment yaml file and return the contents as a dict."""
        if computing_environment_path is None:
            return {"computing_environment": "local", "container_engine": "undefined"}
        filepath = computing_environment_path.resolve()
        if not filepath.is_file():
            raise FileNotFoundError(
                "Computing environment is expected to be a path to an existing"
                f" yaml file. Input was: '{computing_environment_path}'"
            )
        return load_yaml(filepath)

    @staticmethod
    def _load_input_data_paths(input_data: Path) -> List[Path]:
        file_list = [Path(filepath).resolve() for filepath in load_yaml(input_data).values()]
        missing = [str(file) for file in file_list if not file.exists()]
        if missing:
            raise RuntimeError(f"Cannot find input data: {missing}")
        return file_list
