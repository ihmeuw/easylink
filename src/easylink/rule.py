from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional


class Rule(ABC):
    """
    Abstract class to define interface between Steps and Implementations
    and 'Rules' in Snakemake syntax that must be written to the Snakefile
    """

    def write_to_snakefile(self, snakefile_path) -> None:
        with open(snakefile_path, "a") as f:
            f.write(self._build_rule())

    @abstractmethod
    def _build_rule(self) -> str:
        "What actually gets written to the Snakefile. Must be implemented by subclasses."
        pass


@dataclass
class TargetRule(Rule):
    """
    A rule that defines the final output of the pipeline
    Snakemake will determine the DAG based on this target.

    Parameters:
    target_files: List of file paths
    validation: name of file created by InputValidationRule
    """

    target_files: List[str]
    validation: str
    requires_spark: bool

    def _build_rule(self) -> str:
        rulestring = f"""
rule all:
    message: 'Grabbing final output'
    input:
        final_output={self.target_files},
        validation='{self.validation}',"""
        if self.requires_spark:
            rulestring += f"""
        term="spark_logs/spark_master_terminated.txt",
        master_log="spark_logs/spark_master_log.txt",
        worker_logs=gather.num_workers("spark_logs/spark_worker_log_{{scatteritem}}.txt",
        ),
            """
        return rulestring


@dataclass
class ImplementedRule(Rule):
    """
    A rule that defines the execution of an implementation

    Parameters:
    step_name: Name of step
    implementation_name: Name of implementation
    execution_input: List of file paths required by implementation
    validation: name of file created by InputValidationRule to check for compatible input
    output: List of file paths created by implementation
    resources: Computational resources used by executor (e.g. SLURM)
    envvars: Dictionary of environment variables to set
    diagnostics_dir: Directory for diagnostic files
    image_path: Path to Singularity image
    script_cmd: Command to execute
    """

    step_name: str
    implementation_name: str
    input_slots: Dict[str, List[str]]
    validation: str
    output: List[str]
    resources: Optional[dict]
    envvars: dict
    diagnostics_dir: str
    image_path: str
    script_cmd: str
    requires_spark: bool

    def _build_rule(self) -> str:
        return self._build_io() + self._build_resources() + self._build_shell_command()

    def _build_io(self) -> str:
        return (
            f"""
rule:
    name: "{self.implementation_name}"
    message: "Running {self.step_name} implementation: {self.implementation_name}" """
            + self._build_input()
            + f"""        
    output: {self.output}
    log: "{self.diagnostics_dir}/{self.implementation_name}-output.log"
    container: "{self.image_path}" """
        )

    def _build_input(self) -> str:
        input_str = f"""
    input:"""
        for slot_name, slot_files in self.input_slots.items():
            input_str += f"""
        {slot_name.lower()}={slot_files},"""
        input_str += f"""
        validation="{self.validation}", """
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
        slurm_extra="--output '{self.diagnostics_dir}/{self.implementation_name}-slurm-%j.log'"
        """

    def _build_shell_command(self) -> str:
        shell_cmd = f"""
    shell:
        '''
        export DUMMY_CONTAINER_OUTPUT_PATHS={",".join(self.output)}
        export DUMMY_CONTAINER_DIAGNOSTICS_DIRECTORY={self.diagnostics_dir}"""
        for slot_name, slot_files in self.input_slots.items():
            shell_cmd += f"""
        export {slot_name}={",".join(slot_files)}"""
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
    """
    A rule that validates input files against validator for
    an implementation or the final output

    Parameters:
    name: Name
    input: List of file paths to validate
    output: file path to touch on successful validation. Must be used as an input for next rule.
    validator: Callable that takes a file path as input. Raises an error if invalid.
    """

    name: str
    input: List[str]
    output: str
    validator: Callable

    def _build_rule(self) -> str:
        return f"""
rule:
    name: "{self.name}_validator"
    input: {self.input}
    output: touch("{self.output}")
    localrule: True         
    message: "Validating {self.name} input"
    run:
        for f in input:
            validation_utils.{self.validator.__name__}(f)"""
