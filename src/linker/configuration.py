import os
from pathlib import Path
from typing import Dict, Set, Union

import yaml
from loguru import logger

STEP_ORDER = set(
    [
        "full_entity_resolution",
    ]
)


class Config:
    """A container for configuration information where each value is exposed
    as an attribute.

    """

    def __init__(self, pipeline_path: str, computing_environment_input: str):

        self.pipeline_path = Path(pipeline_path)
        if computing_environment_input == "local":
            self.computing_environment_path = None
        else:
            self.computing_environment_path = Path(computing_environment_input)
        self.pipeline = self._load_yaml(pipeline_path)
        self.environment = self._load_computing_environment(computing_environment_input)
        self.computing_environment = self.environment["computing_environment"]
        self.container_engine = self.environment.get("container_engine", None)
        self.steps = self._get_steps()

    def get_step(self, pipeline_step: str) -> Path:
        # TODO: move this into proper config validator
        implementation = self.pipeline["steps"][pipeline_step]["implementation"]
        if implementation == "pvs_like_python":
            # TODO: stop hard-coding filepaths
            step_dir = (
                Path(os.path.realpath(__file__)).parent.parent.parent
                / "steps"
                / "pvs_like_case_study_sample_data"
            )
        else:
            raise NotImplementedError(f"No support for impementation '{implementation}'")
        return step_dir

    def get_resources(self) -> Dict[str, str]:
        return {
            **self.environment["implementation_resources"],
            **self.environment[self.environment["computing_environment"]],
        }

    ####################
    # Helper Functions #
    ####################

    def _load_yaml(self, path: Path) -> Dict:
        with open(path, "r") as file:
            data = yaml.safe_load(file)
        return data

    def _load_computing_environment(self, input: str) -> Dict[str, Union[Dict, str]]:
        if input == "local":
            return {"computing_environment": "local"}
        else:
            path = Path(input).resolve()
            if not path.is_file():
                raise RuntimeError(
                    "Computing environment is expected to be either 'local' or a path "
                    f"to an existing yaml file. Input is neither: '{input}'"
                )
        return self._load_yaml(path)

    def _get_steps(self) -> Set:
        spec_steps = set([x for x in self.pipeline["steps"]])
        steps = set([x for x in spec_steps if x in STEP_ORDER])
        unknown_steps = spec_steps.difference(STEP_ORDER)
        if unknown_steps:
            logger.warning(
                f"Unknown steps are included in the pipeline specification: {unknown_steps}.\n"
                f"Support steps: {STEP_ORDER}"
            )
        return steps
