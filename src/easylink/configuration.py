from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from layered_config_tree import LayeredConfigTree

from easylink.pipeline_schema import PIPELINE_SCHEMAS, PipelineSchema
from easylink.utilities.data_utils import load_yaml
from easylink.utilities.general_utils import exit_with_validation_error

PIPELINE_ERRORS_KEY = "PIPELINE ERRORS"
INPUT_DATA_ERRORS_KEY = "INPUT DATA ERRORS"
ENVIRONMENT_ERRORS_KEY = "ENVIRONMENT ERRORS"

DEFAULT_ENVIRONMENT = {
    "environment": {
        "computing_environment": "local",
        "container_engine": "undefined",
        "implementation_resources": {
            "memory": 1,  # GB
            "cpus": 1,
            "time_limit": 1,  # hours
        },
    }
}
SPARK_DEFAULTS = {
    "workers": {
        "num_workers": 2,
        "cpus_per_node": 1,
        "mem_per_node": 1,  # GB
        "time_limit": 1,  # hours
    },
    "keep_alive": False,
}

# Allow some buffer so that slurm doesn't kill spark workers
SLURM_SPARK_MEM_BUFFER = 500


def load_params_from_specification(
    pipeline_specification: str, input_data: str, computing_environment: str, results_dir: str
) -> Dict[str, Any]:
    """Gather together the pipeline, input data, and computing environment specifications
    into a single dictionary for insertion into the Config object."""
    return {
        "pipeline": load_yaml(pipeline_specification),
        "input_data": _load_input_data_paths(input_data),
        "environment": _load_computing_environment(computing_environment),
        "results_dir": Path(results_dir),
    }


def _load_input_data_paths(
    input_data_specification_path: Union[str, Path]
) -> Dict[str, List[Path]]:
    """Create dictionary of input data paths from the input data yaml file."""
    input_data_paths = load_yaml(input_data_specification_path)
    if not isinstance(input_data_paths, dict):
        raise TypeError(
            "Input data should be submitted like 'key': path/to/file. "
            f"Input was: '{input_data_paths}'"
        )
    filepath_dict = {
        filename: Path(filepath).resolve() for filename, filepath in input_data_paths.items()
    }
    return filepath_dict


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


class Config(LayeredConfigTree):
    """A container for configuration information where each value is exposed
    as an attribute. This class combines the pipeline, input data, and computing
    environment specifications into a single object. It is also responsible for
    validating these specifications.
    """

    def __init__(
        self,
        config_params: Dict[str, Any],
    ):
        super().__init__(layers=["initial_data", "default", "user_configured"])
        self.update(DEFAULT_ENVIRONMENT, layer="default")
        self.update(config_params, layer="user_configured")
        self.update_implementation_configs(self.pipeline)
        self.update({"environment": {"spark": SPARK_DEFAULTS}}, layer="default")
        if self.environment.computing_environment == "slurm":
            # Set slurm defaults to empty dict instead of None so that we don't get errors
            # In slurm resources property
            self.update({"environment": {"slurm": {}}}, layer="default")

        self.update({"schema": self._get_schema()}, layer="initial_data")
        self._validate()
        self.freeze()

    @property
    def computing_environment(self) -> Dict[str, Any]:
        """Generally either 'local' or 'slurm'."""
        return self.environment.computing_environment

    @property
    def slurm(self) -> Dict[str, Any]:
        """A dictionary of Slurm configuration settings."""
        if not self.environment.computing_environment == "slurm":
            return {}
        else:
            return self.environment.slurm.to_dict()

    @property
    def spark(self) -> Dict[str, Any]:
        """A dictionary of spark configuration settings."""
        return self.environment.spark.to_dict()

    @property
    def slurm_resources(self) -> Dict[str, str]:
        """Return the slurm resources as a flat dictionary in format required by snakemake."""
        if not self.computing_environment == "slurm":
            return {}
        raw_slurm_resources = {
            **self.slurm,
            **self.environment.implementation_resources.to_dict(),
        }
        return {
            "slurm_account": f"'{raw_slurm_resources.get('account')}'",
            "slurm_partition": f"'{raw_slurm_resources.get('partition')}'",
            "mem_mb": int(raw_slurm_resources.get("memory", 0) * 1024),
            "runtime": int(raw_slurm_resources.get("time_limit") * 60),
            "cpus_per_task": raw_slurm_resources.get("cpus"),
        }

    @property
    def spark_resources(self) -> Dict[str, Any]:
        """Return the spark resources as a flat dictionary in format rquired by snakemake."""
        spark_workers_raw = self.spark["workers"]
        spark_workers = {
            "num_workers": spark_workers_raw.get("num_workers"),
            "mem_mb": int(spark_workers_raw.get("mem_per_node", 0) * 1024),
            "slurm_mem_mb": int(
                spark_workers_raw.get("mem_per_node", 0) * 1024 + SLURM_SPARK_MEM_BUFFER
            ),
            "runtime": int(spark_workers_raw.get("time_limit") * 60),
            "cpus_per_task": spark_workers_raw.get("cpus_per_node"),
        }
        return {
            **self.slurm,
            **spark_workers,
            **{k: v for k, v in self.spark.items() if k != "workers"},
        }

    #################
    # Setup Methods #
    #################
    def update_implementation_configs(self, level: LayeredConfigTree):
        """Recursively add empty configuration dictionaries to each implementation configuration"""
        if "implementation" in level:
            level.implementation.update({"configuration": {}}, layer="default")
        else:
            for sub_level in level.values():
                if isinstance(sub_level, LayeredConfigTree):
                    self.update_implementation_configs(sub_level)

    def _get_schema(self) -> Optional[PipelineSchema]:
        """Validates the pipeline against supported schemas.

        NOTE: this acts as the pipeline configuration file's validation method since
        we can only find a matching schema if the file is valid.
        """
        errors = defaultdict(dict)
        # Try each schema until one is validated
        for schema in PIPELINE_SCHEMAS:
            logs = schema.validate_step(self.pipeline)
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
        input_data_errors = self.schema.validate_inputs(self.input_data.to_dict())
        if input_data_errors:
            errors[INPUT_DATA_ERRORS_KEY] = input_data_errors
        return errors

    def _validate_environment(self) -> Dict[Any, Any]:
        errors = defaultdict(dict)
        if not self.environment.container_engine in ["docker", "singularity", "undefined"]:
            errors[ENVIRONMENT_ERRORS_KEY]["container_engine"] = [
                f"The value '{self.environment.container_engine}' is not supported."
            ]

        if self.environment.computing_environment == "slurm" and not self.environment.slurm:
            errors[ENVIRONMENT_ERRORS_KEY]["slurm"] = [
                "The environment configuration file must include a 'slurm' key "
                "defining slurm resources if the computing_environment is 'slurm'."
            ]

        return errors
