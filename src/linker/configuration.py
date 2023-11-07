from pathlib import Path
from typing import Dict, List, Optional, Union

from linker.utilities.general_utils import load_yaml


class Config:
    """A container for configuration information where each value is exposed
    as an attribute.

    """

    def __init__(
        self,
        pipeline_specification: str,
        computing_environment: Union[None, str],
        input_data: str,
    ):
        self.pipeline_path = Path(pipeline_specification)
        if computing_environment is None:
            self.computing_environment_path = None
        else:
            self.computing_environment_path = Path(computing_environment)
        self.pipeline = load_yaml(self.pipeline_path)
        self.environment = self._load_computing_environment(self.computing_environment_path)
        self.input_data = self._load_input_data_paths(Path(input_data))
        self.computing_environment = self.environment["computing_environment"]
        self.container_engine = self.environment.get("container_engine", None)

    def get_resources(self) -> Dict[str, str]:
        return {
            **self.environment["implementation_resources"],
            **self.environment[self.environment["computing_environment"]],
        }

    ####################
    # Helper Functions #
    ####################

    @staticmethod
    def _load_computing_environment(
        computing_environment_path: Union[Path, None],
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
    def _load_input_data_paths(input_data: Path) -> List[str]:
        file_list = [Path(filepath).resolve() for filepath in load_yaml(input_data).values()]
        missing = []
        for file in file_list:
            if not file.exists():
                missing.append(str(file))
        if missing:
            raise RuntimeError(f"Cannot find input data: {missing}")
        return file_list
