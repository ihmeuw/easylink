"""
========
Pipeline
========

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
from easylink.utilities.validation_utils import validate_input_file_dummy

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
    any_embarrassingly_parallel
        A boolean indicating whether any implementation in the pipeline is to be
        run in an embarrassingly parallel manner.

    """

    def __init__(self, config: Config):
        self.config = config
        self.pipeline_graph = PipelineGraph(config)
        self.spark_is_required = self.pipeline_graph.spark_is_required
        self.any_embarrassingly_parallel = self.pipeline_graph.any_embarrassingly_parallel

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
            implementation_errors = implementation.validate()
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

    #################################
    # Snakefile Rule Writer Methods #
    #################################

    def _write_imports(self) -> None:
        if not self.any_embarrassingly_parallel:
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
        if self.any_embarrassingly_parallel:
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
        final_output, _ = self.pipeline_graph.get_io_filepaths("results")
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
            input_slot_name="main_input",
            input=final_output,
            output=validator_file,
            validator=validate_input_file_dummy,
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

        input_slots, output_slots = self.pipeline_graph.get_io_slot_attributes(node_name)
        validation_files, validation_rules = self._get_validations(node_name, input_slots)
        for validation_rule in validation_rules:
            validation_rule.write_to_snakefile(self.snakefile_path)

        _input_files, output_files = self.pipeline_graph.get_io_filepaths(node_name)
        embarrassingly_parallel_details = (
            self.pipeline_graph.get_embarrassingly_parallel_details(node_name)
        )

        if embarrassingly_parallel_details["is_embarrassingly_parallel"]:
            # Update the outputs to be the processed chunks
            if len(output_files) > 1:
                raise NotImplementedError(
                    "FIXME [MIC-5883] Multiple output slots/files of EmbarrassinglyParallelSteps not yet supported"
                )
            output_files = [
                str(
                    Path(output_files[0]).parent
                    / "processed/{chunk}"
                    / Path(output_files[0]).name
                )
            ]

            # Create a splitter origin mapper to reduce bottlenecks later.
            # Loop through all implementations and their input slots and generate
            # a mapping of any splitter origin nodes and slots to the names of the
            # implementation and input slot that they are defined at.
            # i.e. {(splitter_origin_node, splitter_origin_slot): (implementation_name, implementation_input_slot)}
            splitter_origin_mapper = {}
            checkpoint_output_dir = None
            for implementation, implementation_node in zip(
                self.pipeline_graph.implementations, self.pipeline_graph.implementation_nodes
            ):
                for input_slot_name, input_slot in implementation.input_slots.items():
                    key = (input_slot.splitter_origin_node, input_slot.splitter_origin_slot)
                    if key == (None, None):  # no splitter origin to map
                        continue
                    if key not in splitter_origin_mapper:
                        splitter_origin_mapper[key] = (implementation_node, input_slot_name)
                    else:
                        raise ValueError(
                            f"Duplicate splitter found for {key}: "
                            f"{splitter_origin_mapper[key]} and {implementation_node}"
                        )

            # Extract where the checkpoint output dir is located
            # FIXME: this won't work for ep sections b/c the output directory
            # for a non-splitting node is definitely not where the checkpoint
            # directory is located
            checkpoint_output_dir = output_files[0].split("/processed/")[0] + "/input_chunks"
            if embarrassingly_parallel_details["is_splitter"]:
                input_slots_to_split = self._get_input_slots_to_split(input_slots)
                if len(input_slots_to_split) > 1:
                    raise NotImplementedError(
                        "FIXME [MIC-5883] Multiple input slots to split not yet supported"
                    )
                input_slot_to_split = input_slots_to_split[0]
                self._write_checkpoint_rule(
                    node_name,
                    input_slots,
                    input_slot_to_split,
                    validation_files,
                    checkpoint_output_dir,
                )

                # Update the input files to be the unprocessed chunks in '/input_chunks/{chunk}'
                input_slots[input_slot_to_split]["filepaths"] = [
                    checkpoint_output_dir + "/{chunk}/result.parquet"
                ]

            if embarrassingly_parallel_details["is_aggregator"]:
                self._write_aggregation_rule(
                    node_name, output_slots, splitter_origin_mapper, checkpoint_output_dir
                )

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
            image_path=implementation.singularity_image_path,
            script_cmd=implementation.script_cmd,
            requires_spark=implementation.requires_spark,
            is_embarrassingly_parallel=embarrassingly_parallel_details[
                "is_embarrassingly_parallel"
            ],
        ).write_to_snakefile(self.snakefile_path)

    def _write_checkpoint_rule(
        self,
        node_name,
        input_slots,
        input_slot_to_split,
        validation_files,
        checkpoint_output_dir,
    ):
        """Writes the snakemake checkpoint rule.

        This build the ``CheckpointRule`` which splits the data into (unprocessed)
        chunks and saves them to this step's '/input_chunks/' subdirectory.
        """
        CheckpointRule(
            name=node_name,
            input_files=input_slots[input_slot_to_split]["filepaths"],
            input_slot_to_split=input_slot_to_split,
            splitter_name=input_slots[input_slot_to_split]["splitter"].__name__,
            validations=validation_files,
            output_dir=checkpoint_output_dir,
        ).write_to_snakefile(self.snakefile_path)

    def _write_aggregation_rule(
        self, node_name, output_slots, splitter_origin_mapper, checkpoint_output_dir
    ):
        """Writes the snakemake aggregation rule."""
        for output_slot_name, output_slot_attrs in output_slots.items():
            # We need to aggregate *each* output slot
            if len(output_slot_attrs["filepaths"]) > 1:
                raise NotImplementedError(
                    "FIXME [MIC-5883] Multiple output slots/files of EmbarrassinglyParallelSteps not yet supported"
                )
            aggregated_output_file = output_slot_attrs["filepaths"][0]
            # Use the mapping to get the checkpoint_filepath
            split_node, splitting_slot = splitter_origin_mapper[
                (
                    output_slot_attrs["splitter_origin_node"],
                    output_slot_attrs["splitter_origin_slot"],
                )
            ]
            checkpoint_rule_name = f"checkpoints.split_{split_node}_{splitting_slot}"
            processed_chunks_to_aggregate = str(
                Path(aggregated_output_file).parent
                / "processed/{chunk}"
                / Path(aggregated_output_file).name
            )
            AggregationRule(
                name=node_name,
                input_files=processed_chunks_to_aggregate,
                output_slot_name=output_slot_name,
                aggregated_output_file=aggregated_output_file,
                aggregator_name=output_slot_attrs["aggregator"].__name__,
                checkpoint_filepath=str(Path(checkpoint_output_dir) / "checkpoint.txt"),
                checkpoint_rule_name=checkpoint_rule_name,
            ).write_to_snakefile(self.snakefile_path)

    @staticmethod
    def _get_validations(
        node_name, input_slots
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
            validation_file = f"input_validations/{node_name}/{input_slot_name}_validator"
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
