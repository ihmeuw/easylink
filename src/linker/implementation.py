from pathlib import Path
from typing import Callable, List

from linker.utilities.general_utils import load_yaml


class Implementation:
    def __init__(self, step_name: str, implementation_name: str):
        self.step_name = step_name
        self.name = implementation_name
        self._directory = self._get_implementation_directory(step_name, implementation_name)
        self._metadata = self._load_metadata()
        self._container_full_stem = self._get_container_full_stem()
        self._validate()

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

    def _load_metadata(self) -> dict:
        metadata_path = self._directory / "metadata.yaml"
        if metadata_path.exists():
            return load_yaml(metadata_path)
        else:
            raise FileNotFoundError(
                f"Could not find metadata file for step '{self.step_name}' at '{metadata_path}'"
            )

    def _get_container_full_stem(self) -> str:
        container_dict = self._metadata["image"]
        return f"{container_dict['path']}/{container_dict['filename']}"

    def _validate(self) -> None:
        """Validates each Implementation"""
        # Check that container exists
        if (
            not Path(f"{self._container_full_stem}.tar.gz").exists()
            and not Path(f"{self._container_full_stem}.sif").exists()
        ):
            raise RuntimeError(f"Container '{self._container_full_stem}' does not exist.")
        # Confirm that the metadata file step matches the pipeline yaml step
        implementation_step = self._metadata["step"]
        pipeline_step = self.step_name  # from pipeline yaml
        if implementation_step != pipeline_step:
            raise RuntimeError(
                f"Implementaton's metadata step '{implementation_step}' does not "
                f"match pipeline configuration's step '{pipeline_step}'"
            )
