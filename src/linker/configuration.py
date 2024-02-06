from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from loguru import logger

from linker.pipeline_schema import PIPELINE_SCHEMAS, PipelineSchema
from linker.utilities.data_utils import load_yaml
from linker.utilities.general_utils import exit_with_validation_error


class Config:
    """A container for configuration information where each value is exposed
    as an attribute.

    """

    def __init__(
        self,
        pipeline_specification: Path,
        input_data: Path,
        computing_environment: Optional[Path],
    ):
        # Handle pipeline specification
        self.pipeline_specification_path = pipeline_specification
        self.pipeline = load_yaml(self.pipeline_specification_path)
        # Handle input data specification
        self.input_data_specification_path = input_data
        self.input_data = self._load_input_data_paths(self.input_data_specification_path)
        # Handle environment specification
        self.computing_environment_specification_path = computing_environment
        self.environment = self._load_computing_environment(
            self.computing_environment_specification_path
        )
        self.computing_environment = self.environment["computing_environment"]
        self.container_engine = self.environment["container_engine"]
        self.implementation_resources = self.environment.get("implementation_resources", {})
        self.slurm = self.environment.get("slurm", {})
        self.spark = self._get_spark_requests(self.environment)

        self.schema = self._get_schema()
        self._validate()

    @property
    def slurm_resources(self) -> Dict[str, str]:
        return {**self.implementation_resources, **self.slurm}

    @property
    def spark_resources(self) -> Dict[str, Any]:
        """Return the spark resources as a flat dictionary"""
        return {
            **self.slurm,
            **self.spark["workers"],
            **{k: v for k, v in self.spark.items() if k != "workers"},
        }

    def get_implementation_name(self, step_name: str) -> str:
        return self.pipeline["steps"][step_name]["implementation"]["name"]

    def get_implementation_config(self, step_name: str) -> Optional[Dict[str, Any]]:
        return self.pipeline["steps"][step_name]["implementation"].get("configuration", None)

    #################
    # Setup Methods #
    #################

    def _get_schema(self) -> Optional[PipelineSchema]:
        """Validates the pipeline against supported schemas."""

        errors = defaultdict(dict)
        for schema in PIPELINE_SCHEMAS:
            logs = []
            config_steps = self.pipeline["steps"].keys()
            # Check that number of schema steps matches number of implementations
            if len(schema.steps) != len(config_steps):
                logs.append(
                    f"Expected {len(schema.steps)} steps but found {len(config_steps)} implementations."
                )
            else:
                for idx, config_step in enumerate(config_steps):
                    # Check that all steps are accounted for and in the correct order
                    schema_step = schema.steps[idx].name
                    if config_step != schema_step:
                        logs.append(
                            f"Step {idx + 1}: the pipeline schema expects step '{schema_step}' "
                            f"but the provided pipeline specifies '{config_step}'. "
                            "Check step order and spelling in the pipeline configuration yaml."
                        )
            if logs:
                errors["PIPELINE ERRORS"][schema.name] = logs
                pass  # try the next schema
            else:
                return schema
        # No schemas were validated
        exit_with_validation_error(dict(errors))

    def _validate(self) -> None:
        errors = {
            **self._validate_files(),
            **self._validate_input_data(),
        }
        if errors:
            exit_with_validation_error(errors)

    def _validate_files(self) -> Dict:
        # TODO [MIC-4723]: validate configuration files
        errors = defaultdict(dict)
        if not self.container_engine in ["docker", "singularity", "undefined"]:
            errors["CONFIGURATION ERRORS"][
                self.computing_environment
            ] = f"Container engine '{self.container_engine}' is not supported."

        if self.spark and self.computing_environment == "local":
            logger.warning(
                "Spark resource requests are not supported in a "
                "local computing environment; these requests will be ignored. The "
                "implementation itself is responsible for spinning up a spark cluster "
                "inside of the relevant container.\n"
                f"Ignored spark cluster requests: {self.spark}"
            )

        return errors

    def _validate_input_data(self) -> Dict:
        errors = defaultdict(dict)
        for input_filepath in self.input_data:
            input_data_errors = self.schema.validate_input(input_filepath)
            if input_data_errors:
                errors["INPUT DATA ERRORS"][str(input_filepath)] = input_data_errors
        return errors

    @staticmethod
    def _load_computing_environment(
        computing_environment_path: Optional[Path],
    ) -> Dict[str, Union[Dict, str]]:
        """Load the computing environment yaml file and return the contents as a dict."""
        if computing_environment_path is None:
            return {"computing_environment": "local", "container_engine": "undefined"}
        filepath = computing_environment_path.resolve()
        if not filepath.is_file():
            raise FileNotFoundError(
                "Computing environment is expected to be a path to an existing"
                f" yaml file. Input was: '{computing_environment_path}'"
            )
        return load_yaml(filepath)

    @staticmethod
    def _load_input_data_paths(input_data_specification_path: Path) -> List[Path]:
        file_list = [
            Path(filepath).resolve()
            for filepath in load_yaml(input_data_specification_path).values()
        ]
        missing = [str(file) for file in file_list if not file.exists()]
        if missing:
            raise RuntimeError(f"Cannot find input data: {missing}")
        return file_list

    @staticmethod
    def _get_spark_requests(environment: Dict[str, Union[Dict, str]]) -> Dict[str, Any]:
        spark = environment.get("spark", {})
        if spark and not "keep_alive" in spark:
            spark["keep_alive"] = False
        return spark
