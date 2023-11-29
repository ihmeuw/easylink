from pathlib import Path
from typing import Callable, Dict, List

from linker.utilities.general_utils import load_yaml


class Implementation:
    def __init__(self, step_name: str, implementation_name: str, container_engine: str):
        self.step_name = step_name
        self.name = implementation_name
        self._container_engine = container_engine
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
            container_full_stem=self._container_full_stem,
        )

    ##################
    # Helper methods #
    ##################

    def _load_metadata(self) -> Dict[str, str]:
        metadata_path = Path(__file__).parent / "implementation_metadata.yaml"
        return load_yaml(metadata_path)

    def _get_container_full_stem(self) -> str:
        if not self.name in self._metadata:
            raise RuntimeError(
                f"Implementation '{self.name}' is not defined in implementation_metadata.yaml"
            )
        return f"{self._metadata[self.name]['path']}/{self._metadata[self.name]['name']}"

    def _validate(self) -> None:
        # TODO [MIC-4709]: Batch all validation errors and log them all at once
        self._validate_expected_step()
        self._validate_container_exists()

    def _validate_expected_step(self):
        implementation_step = self._metadata[self.name]["step"]
        pipeline_step = self.step_name  # from pipeline yaml
        if implementation_step != pipeline_step:
            raise RuntimeError(
                f"Implementaton's metadata step '{implementation_step}' does not "
                f"match pipeline configuration's step '{pipeline_step}'"
            )

    def _validate_container_exists(self):
        # TODO [MIC-4723]: this should be moved into Config
        if not self._container_engine in ["docker", "singularity", "undefined"]:
            raise NotImplementedError(
                f"Container engine '{self._container_engine}' is not supported."
            )
        err_str = f"Container '{self._container_full_stem}' does not exist."
        if (
            self._container_engine == "docker"
            and not Path(f"{self._container_full_stem}.tar.gz").exists()
        ):
            raise RuntimeError(err_str)
        if (
            self._container_engine == "singularity"
            and not Path(f"{self._container_full_stem}.sif").exists()
        ):
            raise RuntimeError(err_str)
        if (
            self._container_engine == "undefined"
            and not Path(f"{self._container_full_stem}.tar.gz").exists()
            and not Path(f"{self._container_full_stem}.sif").exists()
        ):
            raise RuntimeError(err_str)
