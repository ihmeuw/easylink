from pathlib import Path

from linker.step import Step
from linker.utilities.general_utils import load_yaml


class Implementation:
    def __init__(self, step: Step, directory: Path):
        self.step = step
        self.directory = directory
        self.name = directory.name
        self.container_full_stem = self._get_container_full_stem()

    def run(self, runner, container_engine, input_data, results_dir):
        runner(
            container_engine=container_engine,
            input_data=input_data,
            results_dir=results_dir,
            step_name=self.step.name,
            implementation_dir=self.directory,
            container_full_stem=self.container_full_stem,
        )

    ##################
    # Helper methods #
    ##################

    def _get_container_full_stem(self) -> str:
        metadata_path = self.directory / "metadata.yaml"
        if metadata_path.exists():
            metadata = load_yaml(metadata_path)
        else:
            raise FileNotFoundError(
                f"Could not find metadata file for step '{self.step.name}' at '{metadata_path}'"
            )
        container_dict = metadata["image"]
        return f"{container_dict['directory']}/{container_dict['filename']}"
