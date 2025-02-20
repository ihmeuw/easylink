"""
=============
Configuration
=============

This module is responsible for managing an easylink run's configuration as defined
by various user-input specification files.

"""

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
"""The default environment configuration settings."""

SPARK_DEFAULTS = {
    "workers": {
        "num_workers": 2,
        "cpus_per_node": 1,
        "mem_per_node": 1,  # GB
        "time_limit": 1,  # hours
    },
    "keep_alive": False,
}
"""The default spark configuration settings."""

SLURM_SPARK_MEM_BUFFER = 500  # MB


class Config(LayeredConfigTree):
    """A container for configuration information.

    The ``Config`` contains the user-provided specifications for the pipeline,
    input data, and computing environment specifications. It is a nested
    dictionary-like object that supports prioritized layers of configuration settings
    as well as dot-notation access to its attributes.

    The ``Config`` is also reponsible for various validation checks on the provided
    specifications. If any of these are invalid, a validation error is raised with
    as much information as can possibly be provided.

    Parameters
    ----------
    config_params
        A dictionary of all specifications required to run the pipeline. This
        includes the pipeline, input data, and computing environment specifications,
        as well as the results directory.
    potential_schemas
        A list of potential schemas to validate the pipeline configuration against.
        This is primarily used for testing purposes. Defaults to the supported schemas.

    Attributes
    ----------
    environment
        The environment configuration, including computing environment,
        container engine, implementation resources, and slurm- and spark-specific
        requests.
    pipeline
        The pipeline configuration.
    input_data
        The input data filepaths.
    schema
        The :class:`~easylink.pipeline_schema.PipelineSchema` that successfully
        validated the requested pipeline.

    Notes
    -----
    The requested pipeline is checked against a set of supported
    ``PipelineSchemas``. The first schema that successfully validates is assumed
    to be the correct one and is attached to the ``Config`` object and its
    :meth:`~easylink.pipeline_schema.PipelineSchema.configure_pipeline`
    method is called.
    """

    def __init__(
        self,
        config_params: dict[str, Any],
        potential_schemas: PipelineSchema | list[PipelineSchema] = PIPELINE_SCHEMAS,
    ) -> None:
        super().__init__(layers=["initial_data", "default", "user_configured"])
        self.update(DEFAULT_ENVIRONMENT, layer="default")
        self.update(config_params, layer="user_configured")
        self.update({"environment": {"spark": SPARK_DEFAULTS}}, layer="default")
        self.update({"pipeline": {"combined_implementations": {}}}, layer="default")
        if self.environment.computing_environment == "slurm":
            # Set slurm defaults to empty dict instead of None so that we don't get errors
            # in slurm_resources property
            self.update({"environment": {"slurm": {}}}, layer="default")
        if not isinstance(potential_schemas, list):
            potential_schemas = [potential_schemas]
        self.update({"schema": self._get_schema(potential_schemas)}, layer="initial_data")
        self.schema.configure_pipeline(self.pipeline, self.input_data)
        self._validate()
        self.freeze()

    @property
    def computing_environment(self) -> str:
        """The computing environment to run on ('local' or 'slurm')."""
        return self.environment.computing_environment

    @property
    def slurm(self) -> dict[str, Any]:
        """A dictionary of slurm-specific configuration settings."""
        if not self.environment.computing_environment == "slurm":
            return {}
        else:
            return self.environment.slurm.to_dict()

    @property
    def spark(self) -> dict[str, Any]:
        """A dictionary of spark-specific configuration settings."""
        return self.environment.spark.to_dict()

    @property
    def slurm_resources(self) -> dict[str, str]:
        """A flat dictionary of slurm resource requests."""
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
        """A flat dictionary of spark resource requests."""
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

    def _get_schema(self, potential_schemas: list[PipelineSchema]) -> PipelineSchema:
        """Returns the first :class:`~easylink.pipeline_schema.PipelineSchema` that validates the requested pipeline.

        Parameters
        ----------
        potential_schemas
            ``PipelineSchemas`` to validate the pipeline configuration against.

        Returns
        -------
            The first ``PipelineSchema`` that validates the requested pipeline configuration.

        Raises
        ------
        SystemExit
            If the pipeline configuration is not valid for any of the ``potential_schemas``,
            the program exits with a non-zero code and all validation errors found
            are logged.

        Notes
        -----
        This acts as the pipeline configuration file's validation method since
        we can only find a matching ``PipelineSchema`` if that file is valid.

        This method returns the *first* ``PipelineSchema`` that validates and does
        not attempt to check additional ones.
        """
        errors = defaultdict(dict)
        # Try each schema until one is validated
        for schema in potential_schemas:
            logs = schema.validate_step(self.pipeline, self.input_data)
            if logs:
                errors[PIPELINE_ERRORS_KEY][schema.name] = logs
                pass  # try the next schema
            else:  # schema was validated
                return schema
        # No schemas were validated
        exit_with_validation_error(dict(errors))

    def _validate(self) -> None:
        """Validates the ``Config``.

        Raises
        ------
        SystemExit
            If any errors are found, they are batch-logged into a dictionary and
            the program exits with a non-zero code.

        Notes
        -----
        Pipeline validations are handled in :meth:`~easylink.configuration.Config._get_schema`.
        """
        # TODO [MIC-4880]: refactor into validation object
        errors = {
            # NOTE: pipeline configuration validation happens in '_get_schema()'
            **self._validate_input_data(),
            **self._validate_environment(),
        }
        if errors:
            exit_with_validation_error(errors)

    def _validate_input_data(self) -> dict[Any, Any]:
        """Validates the input data configuration.

        Returns
        -------
            A dictionary of input data configuration validation errors.
        """
        errors = defaultdict(dict)
        input_data_dict = self.input_data.to_dict()
        if not input_data_dict:
            errors[INPUT_DATA_ERRORS_KEY] = ["No input data is configured."]
        else:
            input_data_errors = self.schema.validate_inputs(input_data_dict)
            if input_data_errors:
                errors[INPUT_DATA_ERRORS_KEY] = input_data_errors
        return errors

    def _validate_environment(self) -> dict[Any, Any]:
        """Validates the environment configuration.

        Returns
        -------
            A dictionary of environment configuration validation errors.
        """
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
    """Gathers together all specification data.

    This gathers the pipeline, input data, and computing environment specifications
    as well as the results directory into a single dictionary for insertion into
    the :class:`Config` object.

    Parameters
    ----------
    pipeline_specification
        The path to the pipeline specification file.
    input_data
        The path to the input data specification file.
    computing_environment
        The path to the computing environment specification file.
    results_dir
        The path to the results directory.

    Returns
    -------
        A dictionary of all provided specification data.
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
    """Creates a dictionary of input data paths from the input data specification file."""
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
    """Loads the computing environment specification file and returns the contents as a dict."""
    if not computing_environment_specification_path:
        return {}  # handles empty environment.yaml
    elif not Path(computing_environment_specification_path).is_file():
        raise FileNotFoundError(
            "Computing environment is expected to be a path to an existing"
            f" specification file. Input was: '{computing_environment_specification_path}'"
        )
    else:
        return load_yaml(computing_environment_specification_path)
