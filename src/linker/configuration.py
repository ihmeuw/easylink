from pathlib import Path
from typing import Dict, List, Union

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
        self.input_data_path = Path(input_data)
        self.pipeline = load_yaml(self.pipeline_path)
        self.computing_environment = self.environment["computing_environment"]
        self.container_engine = self.environment.get("container_engine", "undefined")

    @property
    def input_data(self) -> List[Path]:
        file_list = [
            Path(filepath).resolve() for filepath in load_yaml(self.input_data_path).values()
        ]
        missing = []
        for file in file_list:
            if not file.exists():
                missing.append(str(file))
        if missing:
            raise RuntimeError(f"Cannot find input data: {missing}")
        return file_list

    def get_resources(self) -> Dict[str, str]:
        return {
            **self.environment["implementation_resources"],
            **self.environment[self.environment["computing_environment"]],
        }

    @property
    def environment(self) -> Dict[str, Union[Dict, str]]:
        """Load the computing environment yaml file and return the contents as a dict."""
        if self.computing_environment_path is None:
            return {"computing_environment": "local", "container_engine": "undefined"}
        filepath = self.computing_environment_path.resolve()
        return load_yaml(filepath)
