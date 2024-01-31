from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

from loguru import logger

from linker.step import Step
from linker.utilities.data_utils import load_yaml
from linker.utilities.slurm_utils import get_slurm_drmaa
from linker.utilities.spark_utils import build_cluster


class Implementation:
    def __init__(
        self,
        step: Step,
        implementation_name: str,
        implementation_config: Optional[Dict[str, Any]],
        container_engine: str,
        resources: Dict[str, Any],
    ):
        self.step = step
        self._pipeline_step_name = step.name
        self.name = implementation_name
        self.config = implementation_config
        self._container_engine = container_engine
        self.resources = resources
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
        intermediate_data: List[Path],
        results_dir: Path,
        diagnostics_dir: Path,
    ) -> None:
        logger.info(f"Running pipeline step ID {step_id}")
        input_data_bindings = self.get_input_bindings(step_id, input_data, intermediate_data)
        if self._requires_spark and session:
            # having an active drmaa session implies we are running on a slurm cluster
            # (i.e. not 'local' computing environment) and so need to spin up a spark
            # cluster instead of relying on the implementation to do it in a container
            drmaa = get_slurm_drmaa()
            spark_master_url, job_id = build_cluster(
                drmaa=drmaa,
                session=session,
                resources=self.resources,
                step_id=step_id,
                results_dir=results_dir,
                diagnostics_dir=diagnostics_dir,
                input_data_bindings=input_data_bindings,
            )
            # Add the spark master url to implementation config
            if not self.config:
                self.config = {}
            self.config["DUMMY_CONTAINER_SPARK_MASTER_URL"] = spark_master_url

        runner(
            container_engine=self._container_engine,
            input_data_bindings=input_data_bindings,
            results_dir=results_dir,
            diagnostics_dir=diagnostics_dir,
            step_id=step_id,
            step_name=self.step_name,
            implementation_name=self.name,
            container_full_stem=self._container_full_stem,
            config=self._formatted_config(self.config),
        )

        if self._requires_spark and session and not self.resources["spark"]["keep_alive"]:
            logger.info(f"Shutting down spark cluster for pipeline step ID {step_id}")
            session.control(job_id, drmaa.JobControlAction.TERMINATE)

        for results_file in results_dir.glob("result.parquet"):
            self.step.validate_output(results_file)

    def validate(self) -> List[Optional[str]]:
        """Validates individual Implementation instances. This is intended to be
        run from the Pipeline validate method.
        """
        logs = []
        logs = self._validate_expected_step(logs)
        logs = self._validate_container_exists(logs)
        logs = self._validate_implementation_config(logs)
        return logs

    ##################
    # Helper methods #
    ##################

    def _formatted_config(self) -> Optional[Dict[str, str]]:
        return self._stringify_keys_values(self.config) if self.config else None

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
            self._container_engine == "docker"
            and not Path(f"{self._container_full_stem}.tar.gz").exists()
        ):
            logs.append(err_str)
        if (
            self._container_engine == "singularity"
            and not Path(f"{self._container_full_stem}.sif").exists()
        ):
            logs.append(err_str)
        if (
            self._container_engine == "undefined"
            and not Path(f"{self._container_full_stem}.tar.gz").exists()
            and not Path(f"{self._container_full_stem}.sif").exists()
        ):
            logs.append(err_str)
        return logs

    def _validate_implementation_config(
        self, logs: List[Optional[str]]
    ) -> List[Optional[str]]:
        """Validate the implementation config against the schema."""
        if self.config:
            for input_path_type in [
                "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                "DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
            ]:
                if input_path_type in self.config:
                    if not isinstance(self.config[input_path_type], list) or not all(
                        isinstance(path, str) for path in self.config[input_path_type]
                    ):
                        logs.append(
                            f"Implementation config error: {input_path_type} must be a non-empty list of input files if configured"
                        )
                    else:
                        for path in self.config[input_path_type]:
                            if not path.startswith(
                                "/input_data/original_input_data"
                            ) or path.startswith("/input_data/original_input_data"):
                                logs.append(
                                    f"Implementation config error: {input_path_type} file {path} must derive from pipeline input data or intermediate data from previous implementation"
                                )
        return logs

    def get_input_bindings(
        self, idx, input_data: List[Path], intermediate_data: List[Path]
    ) -> Dict[str, str]:
        bindings = {}
        config_main_input = (
            self.config.get("DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS", None)
            if self.config
            else None
        )
        config_secondary_input = (
            self.config.get("DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS", None)
            if self.config
            else None
        )
        if config_main_input is None:
            if idx == 0:
                bindings.update(
                    **{
                        f"/input_data/main_input/{file.name}": str(file)
                        for file in intermediate_data
                    }
                )
            else:
                bindings.update(
                    **{
                        f"/input_data/main_input/{file.name}": str(file)
                        for file in input_data
                    }
                )
        if config_main_input or config_secondary_input:
            bindings.update(
                **{
                    f"/input_data/original_input_data/{file.name}": str(file)
                    for file in input_data
                }
            )
            bindings.update(
                **{
                    f"/input_data/intermediate_data/{file.name}": str(file)
                    for file in intermediate_data
                }
            )

        return bindings
