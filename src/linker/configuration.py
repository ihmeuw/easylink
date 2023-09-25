import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import yaml
from loguru import logger

STEP_ORDER = tuple(
    [
        "pvs_like_case_study",
    ]
)


class Config:
    """A container for configuration information where each value is exposed
    as an attribute.

    """

    def __init__(
        self, pipeline_specification: str, computing_environment: Path, input_data: str
    ):
        self.pipeline_path = Path(pipeline_specification)
        if computing_environment is None:
            self.computing_environment_path = None
        else:
            self.computing_environment_path = Path(computing_environment)
        self.pipeline = self._load_yaml(pipeline_specification)
        self.environment = self._load_computing_environment(computing_environment)
        self.input_data = self._load_input_data_paths(input_data)
        self.computing_environment = self.environment["computing_environment"]
        self.container_engine = self.environment.get("container_engine", None)
        self.steps = self._get_steps()

    def get_step_directory(self, step_name: str) -> Path:
        # TODO: move this into proper config validator
        implementation = self.pipeline["steps"][step_name]["implementation"]
        if implementation == "pvs_like_python":
            # TODO: stop hard-coding filepaths
            step_dir = (
                Path(os.path.realpath(__file__)).parent.parent.parent
                / "steps"
                / step_name
                / "implementations"
                / implementation
            )
        else:
            raise NotImplementedError(
                f"No support for step '{step_name}', impementation '{implementation}'."
            )
        return step_dir

    def get_resources(self) -> Dict[str, str]:
        return {
            **self.environment["implementation_resources"],
            **self.environment[self.environment["computing_environment"]],
        }

    ####################
    # Helper Functions #
    ####################

    @staticmethod
    def _load_yaml(filepath: Path) -> Dict:
        with open(filepath, "r") as file:
            data = yaml.safe_load(file)
        return data

    def _load_computing_environment(
        self, computing_environment: Optional[Path]
    ) -> Dict[str, Union[Dict, str]]:
        """Load the computing environment yaml file and return the contents as a dict."""
        if computing_environment is None:
            return {"computing_environment": "local", "container_engine": "undefined"}
        filepath = Path(computing_environment).resolve()
        if not filepath.is_file():
            raise RuntimeError(
                "Computing environment is expected to be a path to an existing"
                f" yaml file. Input was: '{computing_environment}'"
            )
        return self._load_yaml(filepath)

    def _load_input_data_paths(self, input_data: str) -> List[Path]:
        file_list = [
            Path(filepath).resolve() for filepath in self._load_yaml(input_data).values()
        ]
        missing = []
        for file in file_list:
            if not file.exists():
                missing.append(str(file))
        if missing:
            raise RuntimeError(f"Cannot find input data: {missing}")
        return file_list

    def _get_steps(self) -> Tuple:
        spec_steps = tuple(self.pipeline["steps"])
        steps = tuple([x for x in spec_steps if x in STEP_ORDER])
        unknown_steps = [x for x in spec_steps if x not in STEP_ORDER]
        if unknown_steps:
            logger.warning(
                f"Unknown steps are included in the pipeline specification: {unknown_steps}.\n"
                f"Supported steps: {STEP_ORDER}"
            )
        return steps
