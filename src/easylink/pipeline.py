from collections import defaultdict
from pathlib import Path

from loguru import logger

from easylink.configuration import Config
from easylink.pipeline_graph import PipelineGraph
from easylink.rule import ImplementedRule, InputValidationRule, TargetRule
from easylink.utilities.general_utils import exit_with_validation_error
from easylink.utilities.paths import SPARK_SNAKEFILE
from easylink.utilities.validation_utils import validate_input_file_dummy

IMPLEMENTATION_ERRORS_KEY = "IMPLEMENTATION ERRORS"


class Pipeline:
    """Abstraction to handle pipeline specification and execution."""

    def __init__(self, config: Config):
        self.config = config
        """The ``Config`` object."""
        self.pipeline_graph = PipelineGraph(config)
        """The ``PipelineGraph`` object."""
        self.spark_is_required = self.pipeline_graph.spark_is_required()
        """A boolean indicating whether the pipeline requires Spark."""

        # TODO [MIC-4880]: refactor into validation object
        self._validate()

    def _validate(self) -> None:
        """Validates the pipeline."""

        errors = {**self._validate_implementations()}

        if errors:
            exit_with_validation_error(errors)

    def _validate_implementations(self) -> dict:
        """Validates each individual Implementation instance."""
        errors = defaultdict(dict)
        for implementation in self.pipeline_graph.implementations:
            implementation_errors = implementation.validate()
            if implementation_errors:
                errors[IMPLEMENTATION_ERRORS_KEY][implementation.name] = implementation_errors
        return errors

    @property
    def snakefile_path(self) -> Path:
        """The path to the dynamically-generated snakefile."""
        return self.config.results_dir / "Snakefile"

    def build_snakefile(self) -> Path:
        if self.snakefile_path.is_file():
            logger.warning("Snakefile already exists, overwriting.")
            self.snakefile_path.unlink()
        self.write_imports()
        self.write_config()
        self.write_target_rules()
        if self.spark_is_required:
            self.write_spark_module()
        for node in self.pipeline_graph.implementation_nodes:
            self.write_implementation_rules(node)
        return self.snakefile_path

    def write_imports(self) -> None:
        with open(self.snakefile_path, "a") as f:
            f.write("from easylink.utilities import validation_utils")

    def write_target_rules(self) -> None:
        """Writes the rule for the final output and its validation"""
        ## The "input" files to the result node/the target rule are the final output themselves.
        final_output, _ = self.pipeline_graph.get_input_output_files("results")
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
            slot_name="main_input",
            input=final_output,
            output=validator_file,
            validator=validate_input_file_dummy,
        )
        target_rule.write_to_snakefile(self.snakefile_path)
        final_validation.write_to_snakefile(self.snakefile_path)

    def write_implementation_rules(self, node: str) -> None:
        """Writes the rules for each implemented step."""
        implementation = self.pipeline_graph.nodes[node]["implementation"]
        _input_files, output_files = self.pipeline_graph.get_input_output_files(node)
        input_slots = self.pipeline_graph.get_input_slots(node)
        diagnostics_dir = Path("diagnostics") / node
        diagnostics_dir.mkdir(parents=True, exist_ok=True)
        resources = (
            self.config.slurm_resources
            if self.config.computing_environment == "slurm"
            else None
        )
        validation_files, validation_rules = self.get_validations(node, input_slots)
        implementation_rule = ImplementedRule(
            name=node,
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
        )
        for validation_rule in validation_rules:
            validation_rule.write_to_snakefile(self.snakefile_path)
        implementation_rule.write_to_snakefile(self.snakefile_path)

    def write_config(self) -> None:
        """Write any configuration settings to the Snakefile.
        Currently only applicable for spark-dependent rules."""
        with open(self.snakefile_path, "a") as f:
            if self.spark_is_required:
                f.write(
                    f"\nscattergather:\n\tnum_workers={self.config.spark_resources['num_workers']},"
                )

    def write_spark_module(self) -> None:
        "'Import' the spark .smk module into the Snakefile."
        slurm_resources = self.config.slurm_resources
        spark_resources = self.config.spark_resources
        with open(self.snakefile_path, "a") as f:
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
        memory={spark_resources['mem_mb']}
                        """
            f.write(module)

    @staticmethod
    def get_validations(node, input_slots) -> tuple[list[str], list[InputValidationRule]]:
        """Get validator file and validation rule for each slot for a given node"""
        validation_files = []
        validation_rules = []

        for input_slot_name, input_slot_attrs in input_slots.items():
            validation_file = f"input_validations/{node}/{input_slot_name}_validator"
            validation_files.append(validation_file)
            validation_rules.append(
                InputValidationRule(
                    name=node,
                    slot_name=input_slot_name,
                    input=input_slot_attrs["filepaths"],
                    output=validation_file,
                    validator=input_slot_attrs["validator"],
                )
            )
        return validation_files, validation_rules
