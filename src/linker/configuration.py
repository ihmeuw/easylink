import os
import shutil
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from loguru import logger

from linker.pipeline_schema import PIPELINE_SCHEMAS, PipelineSchema
from linker.utilities import paths
from linker.utilities.data_utils import load_yaml
from linker.utilities.general_utils import exit_with_validation_error

PIPELINE_ERRORS_KEY = "PIPELINE ERRORS"
INPUT_DATA_ERRORS_KEY = "INPUT DATA ERRORS"
ENVIRONMENT_ERRORS_KEY = "ENVIRONMENT ERRORS"

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
            "num_workers": 2,
            "cpus_per_node": 1,
            "mem_per_node": 1,  # GB
            "time_limit": 1,  # hours
        },
        "keep_alive": False,
    },
}

SLURM_SPARK_MEM_BUFFER = 500


class Config:
    """A container for configuration information where each value is exposed
    as an attribute. This class combines the pipeline, input data, and computing
    environment specifications into a single object. It is also responsible for
    validating these specifications.
    """

    def __init__(
        self,
        pipeline_specification: Union[str, Path],
        input_data: Union[str, Path],
        computing_environment: Optional[Union[str, Path]],
        results_dir: Union[str, Path],
    ):
        # Handle pipeline specification
        self.pipeline = load_yaml(pipeline_specification)
        self._requires_spark = self._determine_if_spark_is_required(self.pipeline)
        # Handle input data specification
        self.input_data = self._load_input_data_paths(input_data)
        # Handle environment specification
        self.environment = self._load_computing_environment(computing_environment)
        # Store path to results directory
        self.results_dir = Path(results_dir)

        # Extract environment attributes and assign defaults as necessary
        self.computing_environment = self._get_required_attribute(
            self.environment, "computing_environment"
        )
        self.container_engine = self._get_required_attribute(
            self.environment, "container_engine"
        )
        self.slurm = self.environment.get("slurm", {})  # no defaults for slurm
        self.implementation_resources = self._get_implementation_resource_requests(
            self.environment
        )
        self.spark = self._get_spark_requests(self.environment, self._requires_spark)

        self.schema = self._get_schema()  # NOTE: must be called prior to self._validate()
        self._validate()

    @property
    def slurm_resources(self) -> Dict[str, str]:
        if not self.computing_environment == "slurm":
            return {}
        raw_slurm_resources = {**self.slurm, **self.implementation_resources}
        return {
            "slurm_account": f"'{raw_slurm_resources.get('account')}'",
            "slurm_partition": f"'{raw_slurm_resources.get('partition')}'",
            "mem_mb": int(raw_slurm_resources.get("memory", 0) * 1024),
            "runtime": int(raw_slurm_resources.get("time_limit") * 60),
            "cpus_per_task": raw_slurm_resources.get("cpus"),
        }

    @property
    def spark_resources(self) -> Dict[str, Any]:
        """Return the spark resources as a flat dictionary"""
        spark_workers_raw = self.spark.get("workers")
        spark_workers = {
            "num_workers": spark_workers_raw.get("num_workers"),
            "mem_mb": int(spark_workers_raw.get("mem_per_node", 0) * 1024),
            "slurm_mem_mb": int(spark_workers_raw.get("mem_per_node", 0) * 1024 +SLURM_SPARK_MEM_BUFFER),
            "runtime": int(spark_workers_raw.get("time_limit") * 60),
            "cpus_per_task": spark_workers_raw.get("cpus_per_node"),
        }
        return {
            **self.slurm,
            **spark_workers,
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
    def _determine_if_spark_is_required(pipeline: Dict[str, Any]) -> bool:
        """Check if the pipeline requires spark resources."""
        implementation_metadata = load_yaml(paths.IMPLEMENTATION_METADATA)
        try:
            implementations = [
                step["implementation"]["name"] for step in pipeline["steps"].values()
            ]
        except Exception as e:
            raise KeyError(
                "The pipeline specification should contain a single 'steps' outer key "
                "and each step should contain an 'implementation' key with a 'name' key."
            ) from e
        for implementation in implementations:
            if implementation_metadata[implementation].get("requires_spark", False):
                return True
        return False

    @staticmethod
    def _load_input_data_paths(input_data_specification_path: Union[str, Path]) -> List[Path]:
        input_data_paths = load_yaml(input_data_specification_path)
        if not isinstance(input_data_paths, dict):
            raise TypeError(
                "Input data should be submitted like 'key': path/to/file. "
                f"Input was: '{input_data_paths}'"
            )
        file_list = [Path(filepath).resolve() for filepath in input_data_paths.values()]
        return file_list

    @staticmethod
    def _load_computing_environment(
        computing_environment_specification_path: Optional[str],
    ) -> Dict[Any, Any]:
        """Load the computing environment yaml file and return the contents as a dict."""
        if not computing_environment_specification_path:
            return {}  # handles empty environment.yaml
        elif not Path(computing_environment_specification_path).is_file():
            raise FileNotFoundError(
                "Computing environment is expected to be a path to an existing"
                f" yaml file. Input was: '{computing_environment_specification_path}'"
            )
        else:
            return load_yaml(computing_environment_specification_path)

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
    def _get_implementation_resource_requests(environment: Dict[Any, Any]) -> Dict[Any, Any]:
        """Extracts the implementation_resources requests from the environment
        and assigns default values if they are not present.
        """
        key = "implementation_resources"
        if not key in environment:
            # This is not strictly a required field so just return an empty dict
            return {}

        # Manually walk through the keys and assign default values if they are not present
        requests = environment[key]
        requests = Config._assign_defaults(key, requests, DEFAULT_ENVIRONMENT[key])
        return requests

    @staticmethod
    def _get_spark_requests(
        environment: Dict[Any, Any], requires_spark: bool = False
    ) -> Dict[Any, Any]:
        """Extracts the spark requests from the environment and assigns default
        values if they are not present.
        """
        key = "spark"

        if not requires_spark:
            # This is not strictly a required field so just return an empty dict
            return {}
        elif not key in environment:
            logger.info(f"Assigning default values for spark: '{DEFAULT_ENVIRONMENT[key]}'")
            return DEFAULT_ENVIRONMENT[key]

        # Manually walk through the keys and assign default values if they are not present
        requests = environment[key]
        # HACK: special case spark workers since it's a nested dict
        if not "workers" in requests:
            # Assign the entire default workers dict
            requests["workers"] = DEFAULT_ENVIRONMENT["spark"]["workers"]
            logger.info(
                f"Assigning default values for spark workers: '{requests['workers']}'"
            )
        else:
            # Handle workers since it's nested
            requests["workers"] = Config._assign_defaults(
                f"{key} workers", requests["workers"], DEFAULT_ENVIRONMENT[key]["workers"]
            )
        requests = Config._assign_defaults(key, requests, DEFAULT_ENVIRONMENT[key])

        return requests

    @staticmethod
    def _assign_defaults(key: str, requests: Dict[str, Any], defaults: Dict[str, Any]):
        """Loop through a single level of dictionary items and replace missing values
        with defaults.
        """
        for default_key, default_value in defaults.items():
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

        # Check that each of the pipeline steps also contains an implementation
        metadata = load_yaml(paths.IMPLEMENTATION_METADATA)
        for step, step_config in self.pipeline["steps"].items():
            if not "implementation" in step_config:
                errors[PIPELINE_ERRORS_KEY][
                    f"step {step}"
                ] = "Does not contain an 'implementation'."
            elif not "name" in step_config["implementation"]:
                errors[PIPELINE_ERRORS_KEY][
                    f"step {step}"
                ] = "The implementation does not contain a 'name'."
            elif not step_config["implementation"]["name"] in metadata:
                errors[PIPELINE_ERRORS_KEY][f"step {step}"] = (
                    f"Implementation '{step_config['implementation']['name']}' is not supported. "
                    f"Supported implementations are: {list(metadata.keys())}."
                )
        if errors:
            exit_with_validation_error(dict(errors))

        # Try each schema until one is validated
        for schema in PIPELINE_SCHEMAS:
            logs = []
            config_steps = self.pipeline["steps"].keys()
            # Check that number of schema steps matches number of implementations
            if len(schema.steps) != len(config_steps):
                logs.append(
                    f"Expected {len(schema.steps)} steps but found {len(config_steps)} implementations. "
                    "Check that all steps are accounted for (and there are no "
                    "extraneous ones) in the pipeline configuration yaml."
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
                errors[PIPELINE_ERRORS_KEY][schema.name] = logs
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
        # Check that input data files exist
        missing = [str(file) for file in self.input_data if not file.exists()]
        for file in missing:
            errors[INPUT_DATA_ERRORS_KEY][str(file)] = "File not found."
        # Check that input data files are valid
        for file in [file for file in self.input_data if file.exists()]:
            input_data_errors = self.schema.validate_input(file)
            if input_data_errors:
                errors[INPUT_DATA_ERRORS_KEY][str(file)] = input_data_errors
        return errors

    def _validate_environment(self) -> Dict[Any, Any]:
        errors = defaultdict(dict)
        if not self.container_engine in ["docker", "singularity", "undefined"]:
            errors[ENVIRONMENT_ERRORS_KEY][
                "container_engine"
            ] = f"The value '{self.container_engine}' is not supported."

        if self.computing_environment == "slurm" and not self.slurm:
            errors[ENVIRONMENT_ERRORS_KEY]["slurm"] = (
                "The environment configuration file must include a 'slurm' key "
                "defining slurm resources if the computing_environment is 'slurm'."
            )

        return errors
