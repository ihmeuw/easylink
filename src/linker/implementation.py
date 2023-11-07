import os
from pathlib import Path
from typing import List

from linker.configuration import Config
from linker.utilities.general_utils import load_yaml


class Implementation:
    def __init__(self, step_name: str, config: Config, allowable_implementations: List[str]):
        self.step_name = step_name
        self.config = config
        self.allowable_implentations = allowable_implementations

    def __repr__(self):
        return f"{self.step_name}_implementation"

    @property
    def implementation_dir(self) -> Path:
        # TODO: move this into proper config validator
        implementation = self.config.pipeline["steps"][self.step_name]["implementation"]
        implementation_dir = (
            Path(os.path.realpath(__file__)).parent
            / "steps"
            / self.step_name
            / "implementations"
        )
        implementation_names = [
            str(d.name) for d in implementation_dir.iterdir() if d.is_dir()
        ]
        if implementation in implementation_names:
            implementation_dir = implementation_dir / implementation
        else:
            raise NotImplementedError(
                f"No support found for step '{self.step_name}', implementation '{implementation}'.\n"
                f"Supported implementations for step '{self.step_name}': {self.allowable_implentations}."
            )
        return implementation_dir

    @property
    def container_full_stem(self) -> str:
        metadata_path = self.implementation_dir / "metadata.yaml"
        if metadata_path.exists():
            metadata = load_yaml(metadata_path)
        else:
            raise FileNotFoundError(
                f"Could not find metadata file for step '{self.step_name}' at '{metadata_path}'"
            )
        container_dict = metadata["image"]
        return f"{container_dict['directory']}/{container_dict['filename']}"

    def run(self, runner, container_engine, input_data, results_dir):
        runner(
            container_engine=container_engine,
            input_data=input_data,
            results_dir=results_dir,
            step_name=self.step_name,
            implementation_dir=self.implementation_dir,
            container_full_stem=self.container_full_stem,
        )
