from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

from loguru import logger

from linker.configuration import Config
from linker.step import Step
from linker.utilities.data_utils import load_yaml
from linker.utilities.slurm_utils import get_slurm_drmaa
from linker.utilities.spark_utils import build_spark_cluster
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from linker.pipeline import Pipeline


class Implementation:
    def __init__(
        self,
        config: Config,
        step: Step,
    ):
        self.step = step
        self.config = config
        self._pipeline_step_name = step.name
        self.name = config.get_implementation_name(step.name)
        self.implementation_config = self._get_implementation_config()
        self._metadata = self._load_metadata()
        self.step_name = self._metadata[self.name]["step"]
        self._requires_spark = self._metadata[self.name].get("requires_spark", False)
        self._container_full_stem = self._get_container_full_stem()

    def __repr__(self) -> str:
        return f"Implementation.{self.step_name}.{self.name}"

    def run(
        self,
        session: Optional["drmaa.Session"],
        runner: Callable,
        step_id: str,
        input_data: List[Path],
        results_dir: Path,
        diagnostics_dir: Path,
    ) -> None:
        logger.info(f"Running pipeline step ID {step_id}")
        if self._requires_spark and session:
            # having an active drmaa session implies we are running on a slurm cluster
            # (i.e. not 'local' computing environment) and so need to spin up a spark
            # cluster instead of relying on the implementation to do it in a container
            drmaa = get_slurm_drmaa()
            spark_resources = self.config.spark_resources
            spark_master_url, job_id = build_spark_cluster(
                drmaa=drmaa,
                session=session,
                config=self.config,
                step_id=step_id,
                results_dir=results_dir,
                diagnostics_dir=diagnostics_dir,
                input_data=input_data,
            )
            # Add the spark master url to implementation config
            self.implementation_config["DUMMY_CONTAINER_SPARK_MASTER_URL"] = spark_master_url

        runner(
            container_engine=self.config.container_engine,
            input_data=input_data,
            results_dir=results_dir,
            diagnostics_dir=diagnostics_dir,
            step_id=step_id,
            step_name=self.step_name,
            implementation_name=self.name,
            container_full_stem=self._container_full_stem,
            implementation_config=self.implementation_config,
        )

        if self._requires_spark and session and not spark_resources["keep_alive"]:
            logger.info(f"Shutting down spark cluster for pipeline step ID {step_id}")
            session.control(job_id, drmaa.JobControlAction.TERMINATE)

        self.step.validate_output(step_id, results_dir)

    def validate(self) -> List[Optional[str]]:
        """Validates individual Implementation instances. This is intended to be
        run from the Pipeline validate method.
        """
        logs = []
        logs = self._validate_expected_step(logs)
        logs = self._validate_container_exists(logs)
        return logs

    ##################
    # Helper methods #
    ##################

    def _get_implementation_config(self) -> Dict[Any, Any]:
        """Extracts and formats the implementation specific config from the pipeline config"""
        config = self.config.get_implementation_config(self._pipeline_step_name)
        return self._stringify_keys_values(config) if config else {}

    StringifiedDictionary = Dict[str, Union[str, "StringifiedDictionary"]]

    def _stringify_keys_values(self, config: Any) -> StringifiedDictionary:
        # Singularity requires env variables be strings
        if isinstance(config, Dict):
            return {
                str(key): self._stringify_keys_values(value) for key, value in config.items()
            }
        else:
            # The last step of the recursion is not a dict but the leaf node's value
            return str(config)

    def _load_metadata(self) -> Dict[str, str]:
        metadata_path = Path(__file__).parent / "implementation_metadata.yaml"
        metadata = load_yaml(metadata_path)
        if not self.name in metadata:
            raise RuntimeError(
                f"Implementation '{self.name}' is not defined in implementation_metadata.yaml"
            )
        return metadata

    def _get_container_full_stem(self) -> str:
        return f"{self._metadata[self.name]['path']}/{self._metadata[self.name]['name']}"

    def _validate_expected_step(self, logs: List[Optional[str]]) -> List[Optional[str]]:
        if self.step_name != self._pipeline_step_name:
            logs.append(
                f"Implementaton metadata step '{self.step_name}' does not "
                f"match pipeline configuration step '{self._pipeline_step_name}'"
            )
        return logs

    def _validate_container_exists(self, logs: List[Optional[str]]) -> List[Optional[str]]:
        err_str = f"Container '{self._container_full_stem}' does not exist."
        if (
            self.config.container_engine == "docker"
            and not Path(f"{self._container_full_stem}.tar.gz").exists()
        ):
            logs.append(err_str)
        if (
            self.config.container_engine == "singularity"
            and not Path(f"{self._container_full_stem}.sif").exists()
        ):
            logs.append(err_str)
        if (
            self.config.container_engine == "undefined"
            and not Path(f"{self._container_full_stem}.tar.gz").exists()
            and not Path(f"{self._container_full_stem}.sif").exists()
        ):
            logs.append(err_str)
        return logs
    
    def get_input_paths(self, pipeline: "Pipeline", results_dir: Path):
        return pipeline.get_input_files(self.name, results_dir)
    
    def get_output_paths(self, pipeline: "Pipeline", results_dir: Path):
        output_dir = pipeline.get_output_dir(self.name, results_dir)
        return [output_dir / "result.parquet"]
    
    @property
    def singularity_image_path(self):
        return self._container_full_stem + ".sif"
