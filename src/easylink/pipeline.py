"""

This module is responsible for the ``Pipeline`` class, whose primary purpose
is to perform validations as well as generate the Snakefile to be used by 
`Snakemake <https://snakemake.readthedocs.io/en/stable/>`_ to execute the pipeline.

"""

from collections import defaultdict
from pathlib import Path

from loguru import logger

from easylink.configuration import Config
from easylink.pipeline_graph import PipelineGraph
from easylink.rule import (
    AggregationRule,
    CheckpointRule,
    ImplementedRule,
    InputValidationRule,
    TargetRule,
)
from easylink.utilities.general_utils import exit_with_validation_error
from easylink.utilities.paths import SPARK_SNAKEFILE

IMPLEMENTATION_ERRORS_KEY = "IMPLEMENTATION ERRORS"


class Pipeline:
    """A convenience class for validations and Snakefile generation.

    Parameters
    ----------
    config
        The :class:`~easylink.configuration.Config` object.

    Attributes
    ----------
    config
        The :class:`~easylink.configuration.Config` object.
    pipeline_graph
        The :class:`~easylink.pipeline_graph.PipelineGraph` object.
    spark_is_required
        A boolean indicating whether the pipeline requires Spark.
    any_auto_parallel
        A boolean indicating whether any implementation in the pipeline is to be
        automatically run in parallel.

    """

    def __init__(self, config: Config):
        self.config = config
        self.pipeline_graph = PipelineGraph(config)
        self.spark_is_required = self.pipeline_graph.spark_is_required
        self.any_auto_parallel = self.pipeline_graph.any_auto_parallel

        # TODO [MIC-4880]: refactor into validation object
        self._validate()

    @property
    def snakefile_path(self) -> Path:
        """The path to the dynamically-generated snakefile."""
        return self.config.results_dir / "Snakefile"

    def build_snakefile(self) -> Path:
        """Generates the Snakefile for this ``Pipeline``.

        This method dynamically builds the Snakefile by generating all necessary
        setup instructions (e.g. imports, configuration settings) as well as
        all rules for each :class:`~easylink.implementation.Implementation` in the
        pipeline and appending them to the Snakefile.

        Returns
        -------
            The path to the Snakefile.

        Notes
        -----
        We use the Snakemake term "rule" to refer to a singular component in a Snakefile
        (i.e. in a Snakemake pipeline) that defines input files, output files,
        and the command to run to create those output files. These rules are generated
        dynamically as strings and appended to the Snakefile.
        """
        if self.snakefile_path.is_file():
            logger.warning("Snakefile already exists, overwriting.")
            self.snakefile_path.unlink()
        self._write_imports()
        self._write_wildcard_constraints()
        self._write_spark_config()
        self._write_target_rules()
        self._write_spark_module()
        for node_name in self.pipeline_graph.implementation_nodes:
            self._write_implementation_rules(node_name)
        checkpoint_filepaths = self._get_checkpoint_filepaths()
        for node_name in self.pipeline_graph.splitter_nodes:
            self._write_checkpoint_rule(node_name, checkpoint_filepaths[node_name])
        for node_name in self.pipeline_graph.aggregator_nodes:
            self._write_aggregation_rule(
                node_name,
                checkpoint_filepaths[
                    self.pipeline_graph.nodes[node_name]["implementation"].splitter_node_name
                ],
            )
        return self.snakefile_path

    ##################
    # Helper Methods #
    ##################

    def _validate(self) -> None:
        """Validates the pipeline.

        Raises
        ------
        SystemExit
            If any errors are found, they are batch-logged into a dictionary and
            the program exits with a non-zero code.
        """

        errors = {**self._validate_implementations()}

        if errors:
            exit_with_validation_error(errors)

    def _validate_implementations(self) -> dict:
        """Validates each individual :class:`~easylink.implementation.Implementation` instance.

        Returns
        -------
            A dictionary of ``Implementation`` validation errors.
        """
        errors = defaultdict(dict)
        for implementation in self.pipeline_graph.implementations:
            implementation_errors = implementation.validate(
                skip_image_validation=(self.config.command == "generate_dag"),
                images_dir=self.config.images_dir,
            )
            if implementation_errors:
                errors[IMPLEMENTATION_ERRORS_KEY][implementation.name] = implementation_errors
        return errors

    @staticmethod
    def _get_input_slots_to_split(
        input_slot_dict: dict[str, dict[str, str | list[str]]]
    ) -> list[str]:
        """Gets any input slots that have a splitter attribute."""
        return [
            slot_name
            for slot_name, slot_attrs in input_slot_dict.items()
            if slot_attrs.get("splitter", None)
        ]

    def _get_checkpoint_filepaths(self) -> dict[str, str]:
        """Gets a checkpoint filepath for each splitter node."""
        checkpoint_filepaths = {}
        for node_name in self.pipeline_graph.splitter_nodes:
            _input_files, output_files = self.pipeline_graph.get_io_filepaths(node_name)
            if len(set(output_files)) > 1:
                raise ValueError(
                    "The list of output files from a CheckpointRule should always be "
                    "length 1; wildcards handle the fact that there are actually "
                    "multiple files."
                )
            # The snakemake checkpoint rule requires the output parent directory
            # to the chunked sub-directories (which are created by the splitter).
            # e.g. if the chunks are eventually going to be written to
            # 'intermediate/split_step_1_python_pandas/{chunk}/result.parquet',
            # we need the output directory 'intermediate/split_step_1_python_pandas'
            checkpoint_filepaths[node_name] = str(
                Path(output_files[0]).parent.parent / "checkpoint.txt"
            )
        return checkpoint_filepaths

    #################################
    # Snakefile Rule Writer Methods #
    #################################

    def _write_imports(self) -> None:
        if not self.any_auto_parallel:
            imports = "from easylink.utilities import validation_utils\n"
        else:
            imports = """import glob
import os

from snakemake.exceptions import IncompleteCheckpointException
from snakemake.io import checkpoint_target

from easylink.utilities import aggregator_utils, splitter_utils, validation_utils\n"""
        with open(self.snakefile_path, "a") as f:
            f.write(imports)

    def _write_wildcard_constraints(self) -> None:
        if self.any_auto_parallel:
            with open(self.snakefile_path, "a") as f:
                f.write(
                    """
wildcard_constraints:
    # never include '/' since those are reserved for filepaths
    chunk="[^/]+",\n"""
                )

    def _write_target_rules(self) -> None:
        """Writes the rule for the final output and its validation.

        The input files to the target rule (i.e. the result node) are the final
        output themselves.
        """
        input_slots, _ = self.pipeline_graph.get_io_slot_attributes("results")

        if len(input_slots) != 1:
            raise ValueError("Results node must have only one input slot")

        input_slot_name = list(input_slots.keys())[0]
        input_slot_attrs = input_slots[input_slot_name]
        final_output = input_slot_attrs["filepaths"]
        validator_file = str("input_validations/final_validator")
        # Snakemake resolves the DAG based on the first rule, so we put the target
        # before the validation
        target_rule = TargetRule(
            target_files=final_output,
            validation=validator_file,
            requires_spark=self.spark_is_required,
        )
        final_validation = InputValidationRule(
            name="results",
            input_slot_name=input_slot_name,
            input=final_output,
            output=validator_file,
            validator=input_slot_attrs["validator"],
        )
        target_rule.write_to_snakefile(self.snakefile_path)
        final_validation.write_to_snakefile(self.snakefile_path)

    def _write_spark_config(self) -> None:
        """Writes configuration settings to the Snakefile.

        Notes
        -----
        This is currently only applicable for spark-dependent pipelines.
        """
        if self.spark_is_required:
            with open(self.snakefile_path, "a") as f:
                f.write(
                    f"\nscattergather:\n\tnum_workers={self.config.spark_resources['num_workers']},"
                )

    def _write_spark_module(self) -> None:
        """Inserts the ``easylink.utilities.spark.smk`` Snakemake module into the Snakefile."""
        if not self.spark_is_required:
            return
        slurm_resources = self.config.slurm_resources
        spark_resources = self.config.spark_resources
        module = f"""
module spark_cluster:
    snakefile: '{SPARK_SNAKEFILE}'
    config: config

use rule * from spark_cluster
use rule terminate_spark from spark_cluster with:
    input: rules.all.input.final_output"""
        if self.config.computing_environment == "slurm":
            module += f"""
use rule start_spark_master from spark_cluster with:
    resources:
        slurm_account={slurm_resources['slurm_account']},
        slurm_partition={slurm_resources['slurm_partition']},
        mem_mb={spark_resources['slurm_mem_mb']},
        runtime={spark_resources['runtime']},
        cpus_per_task={spark_resources['cpus_per_task']},
        slurm_extra="--output 'spark_logs/start_spark_master-slurm-%j.log'"
use rule start_spark_worker from spark_cluster with:
    resources:
        slurm_account={slurm_resources['slurm_account']},
        slurm_partition={slurm_resources['slurm_partition']},
        mem_mb={spark_resources['slurm_mem_mb']},
        runtime={spark_resources['runtime']},
        cpus_per_task={spark_resources['cpus_per_task']},
        slurm_extra="--output 'spark_logs/start_spark_worker-slurm-%j.log'"
    params:
        terminate_file_name=rules.terminate_spark.output,
        user=os.environ["USER"],
        cores={spark_resources['cpus_per_task']},
        memory={spark_resources['mem_mb']}"""

        with open(self.snakefile_path, "a") as f:
            f.write(module)

    def _write_implementation_rules(self, node_name: str) -> None:
        """Writes the rules for each :class:`~easylink.implementation.Implementation`.

        This method writes *all* rules required for a given ``Implementation``,
        e.g. splitters and aggregators (if necessary), validations, and the actual
        rule to run the container itself.

        Parameters
        ----------
        node_name
            The name of the ``Implementation`` to write the rule(s) for.
        """

        is_auto_parallel = self.pipeline_graph.get_whether_auto_parallel(node_name)
        input_slots, _output_slots = self.pipeline_graph.get_io_slot_attributes(node_name)
        validation_files, validation_rules = self._get_validations(
            node_name, input_slots, is_auto_parallel
        )
        for validation_rule in validation_rules:
            validation_rule.write_to_snakefile(self.snakefile_path)

        _input_files, output_files = self.pipeline_graph.get_io_filepaths(node_name)

        implementation = self.pipeline_graph.nodes[node_name]["implementation"]
        diagnostics_dir = Path("diagnostics") / node_name
        diagnostics_dir.mkdir(parents=True, exist_ok=True)
        resources = (
            self.config.slurm_resources
            if self.config.computing_environment == "slurm"
            else None
        )
        ImplementedRule(
            name=node_name,
            step_name=" and ".join(implementation.metadata_steps),
            implementation_name=implementation.name,
            input_slots=input_slots,
            validations=validation_files,
            output=output_files,
            resources=resources,
            envvars=implementation.environment_variables,
            diagnostics_dir=str(diagnostics_dir),
            image_path=self.config.images_dir / implementation.singularity_image_name,
            script_cmd=implementation.script_cmd,
            requires_spark=implementation.requires_spark,
            is_auto_parallel=is_auto_parallel,
        ).write_to_snakefile(self.snakefile_path)

    def _write_checkpoint_rule(self, node_name: str, checkpoint_filepath: str) -> None:
        """Writes the snakemake checkpoint rule.

        This builds the ``CheckpointRule`` which splits the data into (unprocessed)
        chunks and saves them in the output directory using wildcards.
        """
        splitter_func_name = self.pipeline_graph.nodes[node_name][
            "implementation"
        ].splitter_func_name
        input_files, output_files = self.pipeline_graph.get_io_filepaths(node_name)
        if len(output_files) > 1:
            raise ValueError(
                "The list of output files from a CheckpointRule should always be "
                "length 1; wildcards handle the fact that there are actually "
                "multiple files."
            )
        # The snakemake checkpoint rule requires the output parent directory
        # to the chunked sub-directories (which are created by the splitter).
        # e.g. if the chunks are eventually going to be written to
        # 'intermediate/split_step_1_python_pandas/{chunk}/result.parquet',
        # we need the output directory 'intermediate/split_step_1_python_pandas'
        output_dir = str(Path(output_files[0]).parent.parent)
        CheckpointRule(
            name=node_name,
            input_files=input_files,
            splitter_func_name=splitter_func_name,
            output_dir=output_dir,
            checkpoint_filepath=checkpoint_filepath,
        ).write_to_snakefile(self.snakefile_path)

    def _write_aggregation_rule(self, node_name: str, checkpoint_filepath: str) -> None:
        """Writes the snakemake aggregation rule.

        This builds the ``AggregationRule`` which aggregates the processed data
        from the chunks originally created by the ``SplitterRule``.
        """
        _input_slots, output_slots = self.pipeline_graph.get_io_slot_attributes(node_name)
        input_files, output_files = self.pipeline_graph.get_io_filepaths(node_name)
        if len(output_slots) > 1:
            raise NotImplementedError(
                "FIXME [MIC-5883] Multiple output slots/files of AutoParallelSteps not yet supported"
            )
        if len(output_files) > 1:
            raise ValueError(
                "There should always only be a single output file from an AggregationRule."
            )
        implementation = self.pipeline_graph.nodes[node_name]["implementation"]
        output_slot_name = list(output_slots.keys())[0]
        output_slot_attrs = list(output_slots.values())[0]
        if len(output_slot_attrs["filepaths"]) > 1:
            raise NotImplementedError(
                "FIXME [MIC-5883] Multiple output slots/files of AutoParallelSteps not yet supported"
            )
        checkpoint_rule_name = f"checkpoints.{implementation.splitter_node_name}"
        AggregationRule(
            name=f"{node_name}_{output_slot_name}",
            input_files=input_files,
            aggregated_output_file=output_files[0],
            aggregator_func_name=implementation.aggregator_func_name,
            checkpoint_filepath=checkpoint_filepath,
            checkpoint_rule_name=checkpoint_rule_name,
        ).write_to_snakefile(self.snakefile_path)

    @staticmethod
    def _get_validations(
        node_name: str,
        input_slots: dict[str, dict[str, str | list[str]]],
        is_auto_parallel: bool,
    ) -> tuple[list[str], list[InputValidationRule]]:
        """Gets the validation rule and its output filepath for each slot for a given node.

        Parameters
        ----------
        node_name
            The name of the ``Implementation`` to get validation data for.
        input_slots
            The input slot attributes for the given node.

        Returns
        -------
            A tuple of lists containing the validation output paths and rules.
        """
        validation_files = []
        validation_rules = []

        for input_slot_name, input_slot_attrs in input_slots.items():
            # auto-parallel implementations rely on snakemake wildcards
            # TODO: [MIC-5787] - need to support multiple wildcards at once
            validation_file = f"input_validations/{node_name}/{input_slot_name}_validator" + (
                "-{chunk}" if is_auto_parallel else ""
            )
            validation_files.append(validation_file)
            validation_rules.append(
                InputValidationRule(
                    name=node_name,
                    input_slot_name=input_slot_name,
                    input=input_slot_attrs["filepaths"],
                    output=validation_file,
                    validator=input_slot_attrs["validator"],
                )
            )
        return validation_files, validation_rules
