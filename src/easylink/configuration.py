from collections import defaultdict
from pathlib import Path
from typing import Any

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


class Config(LayeredConfigTree):
    """A container for configuration information.

    This class combines the pipeline, input data, and computing environment
    specifications into a single LayeredConfigTree object. It is also responsible
    for validating these specifications.
    """

    def __init__(
        self,
        config_params: dict[str, Any],
    ):
        super().__init__(layers=["initial_data", "default", "user_configured"])
        self.update(DEFAULT_ENVIRONMENT, layer="default")
        self.update(config_params, layer="user_configured")
        self.update({"environment": {"spark": SPARK_DEFAULTS}}, layer="default")
        self.update({"pipeline": {"combined_implementations": {}}}, layer="default")
        if self.environment.computing_environment == "slurm":
            # Set slurm defaults to empty dict instead of None so that we don't get errors
            # In slurm resources property
            self.update({"environment": {"slurm": {}}}, layer="default")

        self.update({"schema": self._get_schema()}, layer="initial_data")
        self.schema.configure_pipeline(self.pipeline, self.input_data)
        self._validate()
        self.freeze()

    @property
    def computing_environment(self) -> dict[str, Any]:
        """The computing environment to run on (generally either 'local' or 'slurm')."""
        return self.environment.computing_environment

    @property
    def slurm(self) -> dict[str, Any]:
        """A dictionary of slurm configuration settings."""
        if not self.environment.computing_environment == "slurm":
            return {}
        else:
            return self.environment.slurm.to_dict()

    @property
    def spark(self) -> dict[str, Any]:
        """A dictionary of spark configuration settings."""
        return self.environment.spark.to_dict()

    @property
    def slurm_resources(self) -> dict[str, str]:
        """A flat dictionary of the slurm resources."""
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
    def spark_resources(self) -> dict[str, Any]:
        """A flat dictionary of the spark resources."""
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

    def _get_schema(self) -> PipelineSchema:
        """Validates the requested pipeline against supported schemas.

        Notes
        -----
        This acts as the pipeline configuration file's validation method since
        we can only find a matching schema if the file is valid.

        We use the first schema that validates the pipeline configuration.
        """
        errors = defaultdict(dict)
        # Try each schema until one is validated
        for schema in PIPELINE_SCHEMAS:
            logs = schema.validate_step(self.pipeline, self.input_data)
            if logs:
                errors[PIPELINE_ERRORS_KEY][schema.name] = logs
                pass  # try the next schema
            else:  # schema was validated
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

    def _validate_input_data(self) -> dict[Any, Any]:
        errors = defaultdict(dict)
        input_data_dict = self.input_data.to_dict()
        if not input_data_dict:
            errors[INPUT_DATA_ERRORS_KEY] = ["No input data is configured."]
        else:
            input_data_errors = self.schema.validate_inputs(self.input_data.to_dict())
            if input_data_errors:
                errors[INPUT_DATA_ERRORS_KEY] = input_data_errors
        return errors

    def _validate_environment(self) -> dict[Any, Any]:
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


def load_params_from_specification(
    pipeline_specification: str,
    input_data: str,
    computing_environment: str | None,
    results_dir: str,
) -> dict[str, Any]:
    """Gather together all specification data.

    This gathers the pipeline, input data, and computing environment specifications
    as well as the results directory into a single dictionary for insertion into
    the Config object.
    """
    return {
        "pipeline": load_yaml(pipeline_specification),
        "input_data": _load_input_data_paths(input_data),
        "environment": _load_computing_environment(computing_environment),
        "results_dir": Path(results_dir),
    }


def _load_input_data_paths(
    input_data_specification_path: str | Path,
) -> dict[str, list[Path]]:
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
    computing_environment_specification_path: str | None,
) -> dict[Any, Any]:
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
