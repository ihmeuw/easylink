from pathlib import Path
from typing import Callable, List

from linker.utilities.general_utils import load_yaml


class Implementation:
    def __init__(self, step_name: str, implementation_name: str):
        self.step_name = step_name
        self.name = implementation_name
        self._directory = self._get_implementation_directory(step_name, implementation_name)
        self._container_full_stem = self._get_container_full_stem()

    def run(
        self,
        runner: Callable,
        container_engine: str,
        input_data: List[Path],
        results_dir: Path,
    ) -> None:
        runner(
            container_engine=container_engine,
            input_data=input_data,
            results_dir=results_dir,
            step_name=self.step_name,
            implementation_name=self.name,
            implementation_dir=self._directory,
            container_full_stem=self._container_full_stem,
        )

    ##################
    # Helper methods #
    ##################

    def _get_implementation_directory(self, step_name: str, implementation_name: str) -> Path:
        return (
            Path(__file__).parent
            / "steps"
            / step_name
            / "implementations"
            / implementation_name
        )

    def _get_container_full_stem(self) -> str:
        metadata_path = self._directory / "metadata.yaml"
        if metadata_path.exists():
            metadata = load_yaml(metadata_path)
        else:
            raise FileNotFoundError(
                f"Could not find metadata file for step '{self.step_name.name}' at '{metadata_path}'"
            )
        container_dict = metadata["image"]
        return f"{container_dict['directory']}/{container_dict['filename']}"
