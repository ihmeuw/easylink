"""
===============
Snakemake Rules
===============

We have chosen to use `Snakemake <https://snakemake.readthedocs.io/en/stable/>`_ 
as the EasyLink workflow manager. This module is responsible for generating the 
Snakemake rules to be run as well as writing them to the Snakefile.

Note we have adopted the Snakemake term "rule" to refer to a singular component 
in a Snakefile (i.e. in a Snakemake pipeline) that defines input files, output files,
and the command to run to create those output files. These rules are generated
dynamically as strings and appended to the Snakefile.
"""

import os
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path


class Rule(ABC):
    """An abstract class used to generate Snakemake rules."""

    def write_to_snakefile(self, snakefile_path) -> None:
        """Writes the rule to the Snakefile.

        Parameters
        ----------
        snakefile_path
            Path to the Snakefile to write the rule to.
        """
        with open(snakefile_path, "a") as f:
            f.write(self.build_rule())

    @abstractmethod
    def build_rule(self) -> str:
        """Builds the snakemake rule to be written to the Snakefile.

        This is an abstract method and must be implemented by concrete instances.
        """
        pass


@dataclass
class TargetRule(Rule):
    """A :class:`Rule` that defines the final output of the pipeline.

    Snakemake will determine the directed acyclic graph (DAG) based on this target.
    """

    target_files: list[str]
    """List of final output filepaths."""
    validation: str
    """Name of file created by :class:`InputValidationRule`."""
    requires_spark: bool
    """Whether or not this rule requires a Spark environment to run."""

    def build_rule(self) -> str:
        """Builds the Snakemake rule for the final output of the pipeline."""
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
    """A :class:`Rule` that defines the execution of an :class:`~easylink.implementation.Implementation`."""

    name: str
    """Name of the rule."""
    step_name: str
    """Name of the step this rule is implementing."""
    implementation_name: str
    """Name of the ``Implementation`` to build the rule for."""
    input_slots: dict[str, dict[str, str | list[str]]]
    """This ``Implementation's`` input slot attributes."""
    validations: list[str]
    """Names of files created by :class:`InputValidationRule`. These files are empty
    but used by Snakemake to build the graph edges of dependency on validation rules."""
    output: list[str]
    """Output data filepaths."""
    resources: dict | None
    """Computational resources used by executor (e.g. SLURM)."""
    envvars: dict
    """Environment variables to set."""
    diagnostics_dir: str
    """Directory for diagnostic files."""
    image_path: str
    """Path to the Singularity image to run."""
    script_cmd: str
    """Command to execute."""
    requires_spark: bool
    """Whether or not this ``Implementation`` requires a Spark environment."""
    is_auto_parallel: bool = False
    """Whether or not this ``Implementation`` is to be automatically run in parallel."""

    def build_rule(self) -> str:
        """Builds the Snakemake rule for this ``Implementation``."""
        if self.is_auto_parallel and len(self.output) > 1:
            raise NotImplementedError(
                "Multiple output slots/files of AutoParallelSteps not yet supported"
            )
        return self._build_io() + self._build_resources() + self._build_shell_cmd()

    def _build_io(self) -> str:
        """Builds the input/output portion of the rule."""
        log_path_chunk_adder = "-{chunk}" if self.is_auto_parallel else ""
        # Handle output files vs directories
        files = [path for path in self.output if Path(path).suffix != ""]
        if len(files) == len(self.output):
            output = self.output
        elif len(files) == 0:
            if len(self.output) != 1:
                raise NotImplementedError("Multiple output directories is not supported.")
            output = f"directory('{self.output[0]}')"
        else:
            raise NotImplementedError(
                "Mixed output types (files and directories) is not supported."
            )
        io_str = (
            f"""
rule:
    name: "{self.name}"
    message: "Running {self.step_name} implementation: {self.implementation_name}" """
            + self._build_input()
            + f"""        
    output: {output}
    log: "{self.diagnostics_dir}/{self.name}-output{log_path_chunk_adder}.log"
    container: "{self.image_path}" """
        )
        return io_str

    def _build_input(self) -> str:
        input_str = f"""
    input:"""
        for slot, attrs in self.input_slots.items():
            env_var = attrs["env_var"].lower()
            input_str += f"""
        {env_var}={attrs["filepaths"]},"""
        input_str += f"""
        validations={self.validations},"""
        if self.requires_spark:
            input_str += f"""
        master_trigger=gather.num_workers(rules.wait_for_spark_worker.output),
        master_url=rules.wait_for_spark_master.output,"""
        return input_str

    def _build_resources(self) -> str:
        """Builds the resources portion of the rule."""
        if not self.resources:
            return ""
        return f"""
    resources:
        slurm_partition={self.resources['slurm_partition']},
        mem_mb={self.resources['mem_mb']},
        runtime={self.resources['runtime']},
        cpus_per_task={self.resources['cpus_per_task']},
        slurm_extra="--output '{self.diagnostics_dir}/{self.name}-slurm-%j.log'" """

    def _build_shell_cmd(self) -> str:
        """Builds the shell command portion of the rule."""
        # TODO [MIC-5787]: handle multiple wildcards, e.g.
        #   output_paths = ",".join(self.output)
        #   wildcards_subdir = "/".join([f"{{wildcards.{wc}}}" for wc in self.wildcards])
        #   and then in shell cmd: export DUMMY_CONTAINER_OUTPUT_PATHS={output_paths}/{wildcards_subdir}

        # snakemake shell commands require wildcards to be prefaced with 'wildcards.'
        output_files = ",".join(self.output).replace("{chunk}", "{wildcards.chunk}")
        shell_cmd = f"""
    shell:
        '''
        export DUMMY_CONTAINER_OUTPUT_PATHS={output_files}
        export DUMMY_CONTAINER_DIAGNOSTICS_DIRECTORY={self.diagnostics_dir}"""
        for input_slot_attrs in self.input_slots.values():
            # snakemake shell commands require wildcards to be prefaced with 'wildcards.'
            input_files = ",".join(input_slot_attrs["filepaths"]).replace(
                "{chunk}", "{wildcards.chunk}"
            )
            shell_cmd += f"""
        export {input_slot_attrs["env_var"]}={input_files}"""
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
    """A :class:`Rule` that validates input files.

    Each file coming into the pipeline via an :class:`~easylink.graph_components.InputSlot`
    must be validated against a specific validator function. This rule is responsible
    for running those validations as well as creating the (empty) validation output
    files that are used by Snakemake to build the graph edge from this rule to the
    next.
    """

    name: str
    """Name of the rule."""
    input_slot_name: str
    """Name of the ``InputSlot``."""
    input: list[str]
    """List of filepaths to validate."""
    output: str
    """Filepath of validation output. It must be used as an input for next rule."""
    validator: Callable | None
    """Callable that takes a filepath as input. Raises an error if invalid."""

    def build_rule(self) -> str:
        """Builds the Snakemake rule for this validation.

        This rule runs the appropriate validator function on each input file as well
        as creates an empty file at the end. This empty file is used by Snakemake
        to build the graph edge from this rule to the next (since the validations
        themselves don't generate any output).
        """
        return f"""
rule:
    name: "{self.name}_{self.input_slot_name}_validator"
    input: {self.input}
    output: touch("{self.output}")
    localrule: True         
    message: "Validating {self.name} input slot {self.input_slot_name}"
    run:
        for f in input:
            validation_utils.{self.validator.__name__}(f)"""


@dataclass
class CheckpointRule(Rule):
    """A :class:`Rule` that defines a checkpoint.

    When running an :class:`~easylink.implementation.Implementation` in an auto
    parallel way, we do not know until runtime how many parallel jobs there will
    be (e.g. we don't know beforehand how many chunks a large incoming dataset will
    be split into since the incoming dataset isn't created until runtime). The
    snakemake mechanism to handle this dynamic nature is a
    `checkpoint <https://snakemake.readthedocs.io/en/stable/snakefiles/rules.html#data-dependent-conditional-execution/>`_
    rule along with a directory as output.

    Notes
    -----
    There is a known `Snakemake bug <https://github.com/snakemake/snakemake/issues/3036>`_
    which prevents the use of multiple checkpoints in a single Snakefile. We
    work around this by generating an empty checkpoint.txt file as part of this
    rule. If this file does not yet exist when trying to run the :class:`AggregationRule`,
    it means that the checkpoint has not yet been executed for the
    particular wildcard value(s). In this case, we manually raise a Snakemake
    ``IncompleteCheckpointException`` which Snakemake automatically handles
    and leads to a re-evaluation after the checkpoint has successfully passed.

    TODO [MIC-5658]: Thoroughly test this workaround when implementing cacheing.
    """

    name: str
    """Name of the rule."""
    input_files: list[str]
    """The input filepaths."""
    splitter_func_name: str
    """The splitter function's name."""
    output_dir: str
    """Output directory path. It must be used as an input for next rule."""
    checkpoint_filepath: str
    """Path to the checkpoint file. This is only needed for the bugfix workaround."""

    def build_rule(self) -> str:
        """Builds the Snakemake rule for this checkpoint.

        Checkpoint rules are a special type of rule in Snakemake that allow for dynamic
        generation of output files. This rule is responsible for splitting the input
        files into chunks. Note that the output of this rule is a Snakemake ``directory``
        object as opposed to a specific file like typical rules have.
        """
        checkpoint = f"""
checkpoint:
    name: "{self.name}"
    input: 
        files={self.input_files},
    output: 
        output_dir=directory("{self.output_dir}"),
        checkpoint_file=touch("{self.checkpoint_filepath}"),
    params:
        input_files=lambda wildcards, input: ",".join(input.files),
    localrule: True
    message: "Splitting {self.name} into chunks"
    run:
        splitter_utils.{self.splitter_func_name}(
            input_files=list(input.files),
            output_dir=output.output_dir,
            desired_chunk_size_mb=0.1,
        )"""
        return checkpoint


@dataclass
class AggregationRule(Rule):
    """A :class:`Rule` that aggregates the processed chunks of output data.

    When running an :class:`~easylink.implementation.Implementation` in an auto
    parallel way, we need to aggregate the output files from each parallel job
    into a single output file.
    """

    name: str
    """Name of the rule."""
    input_files: list[str]
    """The input processed chunk files to aggregate."""
    aggregated_output_file: str
    """The final aggregated results file."""
    aggregator_func_name: str
    """The name of the aggregation function to run."""
    checkpoint_filepath: str
    """Path to the checkpoint file. This is only needed for the bugfix workaround."""
    checkpoint_rule_name: str
    """Name of the checkpoint rule."""

    def build_rule(self) -> str:
        """Builds the Snakemake rule for this aggregator.

        When running an :class:`~easylink.step.AutoParallelStep`, we need
        to aggregate the output files from each parallel job into a single output file.
        This rule relies on a dynamically generated aggregation function which returns
        all of the **processed** chunks (from running the ``AutoParallelStep's``
        container in parallel) and uses them as inputs to the actual aggregation
        rule.

        Notes
        -----
        There is a known `Snakemake bug <https://github.com/snakemake/snakemake/issues/3036>`_
        which prevents the use of multiple checkpoints in a single Snakefile. We
        work around this by generating an empty checkpoint.txt file in the
        :class:`~CheckpointRule`. If this file does not yet exist when trying to
        aggregate, it means that the checkpoint has not yet been executed for the
        particular wildcard value(s). In this case, we manually raise a Snakemake
        ``IncompleteCheckpointException`` which `Snakemake automatically handles
        <https://snakemake.readthedocs.io/en/stable/snakefiles/rules.html#data-dependent-conditional-execution>`_
        and leads to a re-evaluation after the checkpoint has successfully passed,
        i.e. we replicate `Snakemake's behavior <https://github.com/snakemake/snakemake/blob/04f89d330dd94baa51f41bc796392f85bccbd231/snakemake/checkpoints.py#L42>`_.
        """
        input_function = self._define_input_function()
        rule = self._define_aggregator_rule()
        return input_function + rule

    def _define_input_function(self):
        """Builds the `input function <https://snakemake.readthedocs.io/en/stable/snakefiles/rules.html#input-functions>`_."""
        # NOTE: In the f-string below, we serialize the list `self.input_files`
        # into a string which must later be executed as python code (by snakemake).
        # Let's expand the list into a string representation of a python list so that
        # we explicitly rely on `eval(repr(self.input_files)) == self.input_files`.
        input_files_list_str = repr(self.input_files)
        func = f"""
def get_aggregation_inputs_{self.name}(wildcards):
    checkpoint_file = "{self.checkpoint_filepath}"
    if not os.path.exists(checkpoint_file):
        output, _ = {self.checkpoint_rule_name}.rule.expand_output(wildcards)
        raise IncompleteCheckpointException({self.checkpoint_rule_name}.rule, checkpoint_target(output[0]))
    checkpoint_output = glob.glob(f"{{{self.checkpoint_rule_name}.get(**wildcards).output.output_dir}}/*/")
    chunks = [Path(filepath).parts[-1] for filepath in checkpoint_output]
    input_files = []
    for filepath in {input_files_list_str}:
        input_files.extend(expand(filepath, chunk=chunks))
    return input_files"""
        return func

    def _define_aggregator_rule(self):
        """Builds the rule that runs the aggregation."""
        rule = f"""
rule:
    name: "{self.name}"
    input: get_aggregation_inputs_{self.name}
    output: {[self.aggregated_output_file]}
    localrule: True
    message: "Aggregating {self.name}"
    run:
        aggregator_utils.{self.aggregator_func_name}(
            input_files=list(input),
            output_filepath="{self.aggregated_output_file}",
        )"""
        return rule
