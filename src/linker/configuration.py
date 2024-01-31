from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from loguru import logger

from linker.pipeline_schema import PIPELINE_SCHEMAS, PipelineSchema
from linker.utilities.data_utils import load_yaml
from linker.utilities.general_utils import exit_with_validation_error

DEFAULT_ENVIRONMENT_VALUES = load_yaml(Path(__file__).parent / "default_environment.yaml")

REQUIRED_KEYS = ["computing_environment", "container_engine"]


class Config:
    """A container for configuration information where each value is exposed
    as an attribute. This class combines the pipeline, input data, and computing
    environment specifications into a single object. It is also responsible for
    validating these specifications.
    """

    def __init__(
        self,
        pipeline_specification: Path,
        input_data: Path,
        computing_environment: Optional[Path],
    ):
        # Handle pipeline specification
        self.pipeline_specification_path = pipeline_specification
        self.pipeline = load_yaml(pipeline_specification)
        # Handle input data specification
        self.input_data_specification_path = input_data
        self.input_data = self._load_input_data_paths(input_data)
        # Handle computing environment specification
        self.computing_environment_specificaton_path = computing_environment
        self.environment = (
            self._load_computing_environment(computing_environment)
            if computing_environment
            else {}
        )
        self.environment = self._assign_default_environment(
            self.environment, DEFAULT_ENVIRONMENT_VALUES
        )
        self.computing_environment = self.environment.get("computing_environment", {})
        self.container_engine = self.environment.get("container_engine", {})
        self.slurm = self.environment.get("slurm", {})
        self.implementation_resources = self.environment.get("implementation_resources", {})
        self.spark = self.environment.get("spark", {})

        self.schema = self._get_schema()
        self._validate()

    def get_slurm_resources(self) -> Dict[str, str]:
        return {
            **self.environment["implementation_resources"],
            **self.environment[self.environment["computing_environment"]],
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
            # TODO: [MIC-4723] Add config validation
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
        computing_environment_path: Path,
    ) -> Dict[str, Union[Dict, str]]:
        """Load the computing environment yaml file and return the contents as a dict."""
        filepath = computing_environment_path.resolve()
        if not filepath.is_file():
            raise FileNotFoundError(
                "Computing environment is expected to be a path to an existing"
                f" yaml file. Input was: '{computing_environment_path}'"
            )
        environment = load_yaml(filepath)  # handles empty environment.yaml
        return environment if environment else {}

    @staticmethod
    def _load_input_data_paths(input_data: Path) -> List[Path]:
        file_list = [Path(filepath).resolve() for filepath in load_yaml(input_data).values()]
        missing = [str(file) for file in file_list if not file.exists()]
        if missing:
            raise RuntimeError(f"Cannot find input data: {missing}")
        return file_list

    @staticmethod
    def _assign_default_environment(
        subenv: Dict[str, Any], defaults: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Recursively fill in missing values with defaults."""
        outer_keys = DEFAULT_ENVIRONMENT_VALUES.keys()
        for key, default_value in defaults.items():
            if key in REQUIRED_KEYS and key not in subenv:
                # Add missing required item
                logger.info(f"Assigning default value '{default_value}' to '{key}'")
                subenv[key] = default_value
            if key not in REQUIRED_KEYS and key in subenv:
                # If user defined some key, look for missing sub-keys and update
                if isinstance(default_value, dict) and isinstance(subenv[key], dict):
                    # Recursively traverse dictionaries and update as needed
                    subenv[key] = Config._assign_default_environment(
                        subenv[key], default_value
                    )
                elif (
                    isinstance(default_value, dict) and not isinstance(subenv[key], dict)
                ) or (not isinstance(default_value, dict) and isinstance(subenv[key], dict)):
                    # If the default value and the user defined value are not the same type
                    # then the user defined value is incorrect
                    # TODO: [MIC-4723] move this to config validation
                    raise TypeError(
                        f"Expected '{key}' to be of type '{type(default_value).__name__}' but found '{type(subenv[key]).__name__}'"
                    )
                else:
                    # The value is already defined; keep it
                    pass
            if key not in REQUIRED_KEYS and key not in subenv and key not in outer_keys:
                # Add missing optional items
                logger.info(f"Assigning default value '{default_value}' to '{key}'")
                subenv[key] = default_value
        return subenv
