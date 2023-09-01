from typing import  Dict, Optional, Union
from pathlib import Path
import os

import yaml


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
        self.full_config = self._combine_configurations(self.pipeline_path, computing_environment_input)
        # TODO: make pipeline implementation generic
        self.implementation = self.full_config["implementation"]
        self.computing_environment = self.full_config["computing_environment"]
        self.implementation_resources = self.full_config.get("implementation_resources", None)
        self.slurm = self.full_config.get("slurm", None)
                

    def _combine_configurations(self, pipeline_path: Path, env_input: str):
        pipeline = self._load_yaml(pipeline_path)
        compute_env = self._load_computing_environment(env_input)
        return {**pipeline, **compute_env}



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

    

    def get_steps(self) -> Path:
        # TODO: get a handle on exception handling and logging
        if self.implementation == "pvs_like_python":
            # TODO: stop hard-coding filepaths
            step_dir = (
                Path(os.path.realpath(__file__)).parent.parent.parent
                / "steps"
                / "pvs_like_case_study_sample_data"
            )
        else:
            raise NotImplementedError(f"No support for impementation '{self.implementation}'")
        return step_dir
        


"""
get dictionaries of 
- implementation
- computing_environment
- implementation_resources
- slurm
"""
    