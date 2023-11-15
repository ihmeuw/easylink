from pathlib import Path
from typing import Callable, Dict, List

from linker.utilities.general_utils import load_yaml


class Implementation:
    def __init__(self, step_name: str, implementation_name: str):
        self.step_name = step_name
        self.name = implementation_name
        self._metadata = self._load_metadata()
        self._container_location = self._get_container_location()
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
            container_location=self._container_location,
        )

    ##################
    # Helper methods #
    ##################

    def _load_metadata(self) -> Dict[str, str]:
        metadata_path = Path(__file__).parent / "steps" / "implementation_metadata.yaml"
        return load_yaml(metadata_path)

    def _get_container_location(self) -> Path:
        return Path(self._metadata[self.name]["path"])

    def _validate(self) -> None:
        """Validates each Implementation"""

        # Check that the container path exists
        if not Path(self._container_location).exists():
            raise RuntimeError(
                f"Container directory '{self._container_location}' does not exist."
            )

        # Check that container exists
        container_full_stem = f"{self._container_location}/{self.name}"
        if (
            not Path(f"{container_full_stem}.tar.gz").exists()
            and not Path(f"{container_full_stem}.sif").exists()
        ):
            raise RuntimeError(f"Container '{container_full_stem}' does not exist.")

        # Confirm that the metadata file step matches the pipeline yaml step
        implementation_step = self._metadata[self.name]["step"]
        pipeline_step = self.step_name  # from pipeline yaml
        if implementation_step != pipeline_step:
            raise RuntimeError(
                f"Implementaton's metadata step '{implementation_step}' does not "
                f"match pipeline configuration's step '{pipeline_step}'"
            )
