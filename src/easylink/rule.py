import os
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass


class Rule(ABC):
    """Abstract class to define interface between Steps and Implementations.

    This class is responsible for converting the defined interfaces between
    Steps and Implementations to a Snakemake rule and writing it out to the
    snakefile to be run.
    """

    def write_to_snakefile(self, snakefile_path) -> None:
        with open(snakefile_path, "a") as f:
            f.write(self._build_rule())

    @abstractmethod
    def _build_rule(self) -> str:
        """Builds the snakemake rule to be written to the Snakefile.

        This is an abstract method and must be implemented by concrete instances.
        """
        pass


@dataclass
class TargetRule(Rule):
    """A rule that defines the final output of the pipeline.

    Snakemake will determine the DAG based on this target.
    """

    target_files: list[str]
    """List of file paths."""
    validation: str
    """Name of file created by InputValidationRule."""
    requires_spark: bool

    def _build_rule(self) -> str:
        outputs = [os.path.basename(file_path) for file_path in self.target_files]
        rulestring = f"""
rule all:
    message: 'Grabbing final output'
    localrule: True   
    input:
        final_output={self.target_files},
        validation='{self.validation}',"""
        if self.requires_spark:
            rulestring += f"""
        term="spark_logs/spark_master_terminated.txt",
        master_log="spark_logs/spark_master_log.txt",
        worker_logs=gather.num_workers("spark_logs/spark_worker_log_{{scatteritem}}.txt",
        ),"""
        rulestring += f"""
    output: {outputs}
    run:
        import os
        for input_path, output_path in zip(input.final_output, output):
            os.symlink(input_path, output_path)"""
        return rulestring


@dataclass
class ImplementedRule(Rule):
    """A rule that defines the execution of an implementation"""

    name: str
    """Name to give the rule."""
    step_name: str
    """Name of the step."""
    implementation_name: str
    """Name of the implementation."""
    input_slots: dict[str, dict[str, str | list[str]]]
    """List of file paths required by implementation."""
    validations: list[str]
    """Names of files created by InputValidationRule to check for compatible input."""
    output: list[str]
    """List of file paths created by implementation."""
    resources: dict | None
    """Computational resources used by executor (e.g. SLURM)."""
    envvars: dict
    """ Dictionary of environment variables to set."""
    diagnostics_dir: str
    """Directory for diagnostic files."""
    image_path: str
    """Path to Singularity image."""
    script_cmd: str
    """Command to execute"""
    requires_spark: bool
    """Whether this implementation requires a Spark environment."""

    def _build_rule(self) -> str:
        return self._build_io() + self._build_resources() + self._build_shell_command()

    def _build_io(self) -> str:
        return (
            f"""
rule:
    name: "{self.name}"
    message: "Running {self.step_name} implementation: {self.implementation_name}" """
            + self._build_input()
            + f"""        
    output: {self.output}
    log: "{self.diagnostics_dir}/{self.name}-output.log"
    container: "{self.image_path}" """
        )

    def _build_input(self) -> str:
        input_str = f"""
    input:"""
        for slot_attrs in self.input_slots.values():
            input_str += f"""
        {slot_attrs["env_var"].lower()}={slot_attrs["filepaths"]},"""
        input_str += f"""
        validations={self.validations}, """
        if self.requires_spark:
            input_str += f"""
        master_trigger=gather.num_workers(rules.wait_for_spark_worker.output),
        master_url=rules.wait_for_spark_master.output,
            """
        return input_str

    def _build_resources(self) -> str:
        if not self.resources:
            return ""
        return f"""
    resources:
        slurm_partition={self.resources['slurm_partition']},
        mem_mb={self.resources['mem_mb']},
        runtime={self.resources['runtime']},
        cpus_per_task={self.resources['cpus_per_task']},
        slurm_extra="--output '{self.diagnostics_dir}/{self.name}-slurm-%j.log'" """

    def _build_shell_command(self) -> str:
        shell_cmd = f"""
    shell:
        '''
        export DUMMY_CONTAINER_OUTPUT_PATHS={",".join(self.output)}
        export DUMMY_CONTAINER_DIAGNOSTICS_DIRECTORY={self.diagnostics_dir}"""
        for slot_attrs in self.input_slots.values():
            shell_cmd += f"""
        export {slot_attrs["env_var"]}={",".join(slot_attrs["filepaths"])}"""
        if self.requires_spark:
            shell_cmd += f"""
        read -r DUMMY_CONTAINER_SPARK_MASTER_URL < {{input.master_url}}
        export DUMMY_CONTAINER_SPARK_MASTER_URL"""
        for var_name, var_value in self.envvars.items():
            shell_cmd += f"""
        export {var_name}={var_value}"""
        # Log stdout/stderr to diagnostics directory
        shell_cmd += f"""
        {self.script_cmd} > {{log}} 2>&1
        '''"""

        return shell_cmd


@dataclass
class InputValidationRule(Rule):
    """A rule that validates input files against validator for an implementation or the final output"""

    name: str
    """Name of the rule."""
    slot_name: str
    """Name of the input slot."""
    input: list[str]
    """List of file paths to validate."""
    output: str
    """File path to touch on successful validation. It must be used as an input for next rule."""
    validator: Callable
    """Callable that takes a file path as input. Raises an error if invalid."""

    def _build_rule(self) -> str:
        return f"""
rule:
    name: "{self.name}_{self.slot_name}_validator"
    input: {self.input}
    output: touch("{self.output}")
    localrule: True         
    message: "Validating {self.name} input slot {self.slot_name}"
    run:
        for f in input:
            validation_utils.{self.validator.__name__}(f)"""
