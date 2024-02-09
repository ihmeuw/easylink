from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger

from linker.pipeline_schema import PIPELINE_SCHEMAS, PipelineSchema
from linker.utilities.data_utils import load_yaml
from linker.utilities.general_utils import exit_with_validation_error

DEFAULT_ENVIRONMENT = {
    "computing_environment": "local",
    "container_engine": "undefined",
    "implementation_resources": {
        "memory": 1,  # GB
        "cpus": 1,
        "time_limit": 1,  # hours
    },
    "spark": {
        "workers": {
            "num_workers": 2,  # num_workers + 1 nodes will be requested
            "cpus_per_node": 1,
            "mem_per_node": 1,  # GB
            "time_limit": 1,  # hours
        },
        "keep_alive": False,
    },
}


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
        # Handle environment specification
        self.computing_environment_specification_path = computing_environment
        self.environment = self._load_computing_environment(computing_environment)

        # Extract environment attributes and assign defaults as necessary
        self.computing_environment = self._get_required_attribute(
            self.environment, "computing_environment"
        )
        self.container_engine = self._get_required_attribute(
            self.environment, "container_engine"
        )
        self.slurm = self.environment.get("slurm", {})  # no defaults for slurm
        self.implementation_resources = self._get_requests(
            self.environment, "implementation_resources"
        )
        self.spark = self._get_requests(self.environment, "spark")

        self.schema = self._get_schema()  # NOTE: must be called prior to self._validate()
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

    def get_implementation_specific_configuration(self, step_name: str) -> Dict[str, Any]:
        """Extracts and formats an implementation-specific configuration from the pipeline config"""

        def _stringify_keys_values(config: Any) -> Dict[str, Any]:
            # Singularity requires env variables be strings
            if isinstance(config, Dict):
                return {
                    str(key): _stringify_keys_values(value) for key, value in config.items()
                }
            else:
                # The last step of the recursion is not a dict but the leaf node's value
                return str(config)

        config = self.pipeline["steps"][step_name]["implementation"].get("configuration", {})
        return _stringify_keys_values(config)

    def get_implementation_name(self, step_name: str) -> str:
        return self.pipeline["steps"][step_name]["implementation"]["name"]

    #################
    # Setup Methods #
    #################

    @staticmethod
    def _load_input_data_paths(input_data_specification_path: Path) -> List[Path]:
        file_list = [
            Path(filepath).resolve()
            for filepath in load_yaml(input_data_specification_path).values()
        ]
        return file_list

    @staticmethod
    def _load_computing_environment(
        computing_environment_path: Optional[Path],
    ) -> Dict[Any, Any]:
        """Load the computing environment yaml file and return the contents as a dict."""
        if computing_environment_path:
            if not computing_environment_path.is_file():
                raise FileNotFoundError(
                    "Computing environment is expected to be a path to an existing"
                    f" yaml file. Input was: '{computing_environment_path}'"
                )
            environment = load_yaml(computing_environment_path)
        else:
            environment = {}  # handles empty environment.yaml
        return environment

    @staticmethod
    def _get_required_attribute(environment: Dict[Any, Any], key: str) -> str:
        """Extracts the required-to-run (non-dict) values from the environment
        and assigns default values if they are not present.
        """
        if not key in environment:
            value = DEFAULT_ENVIRONMENT[key]
            logger.info(f"Assigning default value for {key}: '{value}'")
        else:
            value = environment[key]
        return value

    @staticmethod
    def _get_requests(environment: Dict[Any, Any], key: str) -> Dict[Any, Any]:
        """Extracts the requests from the environment and assigns default values
        if they are not present.
        """
        if not key in environment:
            # This is not strictly a required field so just return an empty dict
            return {}
        # Manually walk through the keys and assign default values if they are not present
        requests = environment[key]
        # HACK: special case spark workers since it's a nested dict
        if key == "spark" and not "workers" in requests:
            # Assign the entire default workers dict
            requests["workers"] = DEFAULT_ENVIRONMENT["spark"]["workers"]
            logger.info(
                f"Assigning default values for spark workers: '{requests['workers']}'"
            )
        else:
            for default_key, default_value in DEFAULT_ENVIRONMENT[key].items():
                if not default_key in requests:
                    requests[default_key] = default_value
                    logger.info(
                        f"Assigning default value for {key} {default_key}: '{default_value}'"
                    )
        return requests

    def _get_schema(self) -> Optional[PipelineSchema]:
        """Validates the pipeline against supported schemas.

        NOTE: this acts as the pipeline configurat file's validation method since
        we can only find a matching schema if the file is valid.
        """

        errors = defaultdict(dict)
        error_key = "PIPELINE ERRORS"

        # Check that the pipeline specification contains a single "steps" outer key
        if not len(self.pipeline) == 1 or not "steps" in self.pipeline:
            errors[error_key][
                "generic"
            ] = "The pipeline specification should contain a single 'steps' key."
            exit_with_validation_error(dict(errors))

        # Check that each of the pipeline steps also contains an implementation
        for step, step_config in self.pipeline["steps"].items():
            if not "implementation" in step_config:
                errors[error_key][f"step {step}"] = "Does not contain an 'implementation'."
            elif not "name" in step_config["implementation"]:
                errors[error_key][
                    f"step {step}"
                ] = "The implementation does not contain a 'name'."
        if errors:
            exit_with_validation_error(dict(errors))

        # Try each schema until one is validated
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
                errors[error_key][schema.name] = logs
                pass  # try the next schema
            else:
                return schema
        # No schemas were validated
        exit_with_validation_error(dict(errors))

    def _validate(self) -> None:
        # TODO [MIC-4880]: refactor into validation object
        errors = {
            # NOTE: pipeline configuration validation happens in '_get_schema()'
            **self._validate_input_data(),
            **self._validate_environment(),
        }
        if errors:
            exit_with_validation_error(errors)

    def _validate_input_data(self) -> Dict[Any, Any]:
        errors = defaultdict(dict)
        error_key = "INPUT DATA ERRORS"
        # Check that input data files exist
        missing = [str(file) for file in self.input_data if not file.exists()]
        for file in missing:
            errors[error_key][str(file)] = "File not found."
        # Check that input data files are valid
        for file in [file for file in self.input_data if file.exists()]:
            input_data_errors = self.schema.validate_input(file)
            if input_data_errors:
                errors[error_key][str(file)] = input_data_errors
        return errors

    def _validate_environment(self) -> Dict[Any, Any]:
        errors = defaultdict(dict)
        error_key = "ENVIRONMENT ERRORS"
        if not self.container_engine in ["docker", "singularity", "undefined"]:
            errors[error_key][
                self.computing_environment
            ] = f"Container engine '{self.container_engine}' is not supported."

        if self.spark and self.computing_environment == "local":
            # Just warn, don't actually fail the validation
            logger.warning(
                "Spark resource requests are not supported in a "
                "local computing environment; these requests will be ignored. The "
                "implementation itself is responsible for spinning up a spark cluster "
                "inside of the relevant container.\n"
                f"Ignored spark cluster requests: {self.spark}"
            )

        return errors
