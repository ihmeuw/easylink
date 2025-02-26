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

    @staticmethod
    def get_input_slots_to_split(input_slots) -> list[str]:
        input_slots_to_split = [
            slot_name
            for slot_name, slot_attrs in input_slots.items()
            if slot_attrs.get("splitter", None)
        ]
        return input_slots_to_split


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
    is_embarrassingly_parallel: bool = False
    """Whether or not this ``Implementation`` is to be run in an embarrassingly 
    parallel way."""

    def build_rule(self) -> str:
        """Builds the Snakemake rule for this ``Implementation``."""
        return self._build_io() + self._build_resources() + self._build_shell_cmd()

    def _build_io(self) -> str:
        """Builds the input/output portion of the rule."""
        if self.is_embarrassingly_parallel:
            # Processed chunks are sent to a 'processed' subdir
            output_files = [
                os.path.dirname(file_path)
                + "/processed/{chunk}/"
                + os.path.basename(file_path)
                for file_path in self.output
            ]
            log_path_chunk_adder = "-{chunk}"
        else:
            output_files = self.output
            log_path_chunk_adder = ""

        io_str = (
            f"""
rule:
    name: "{self.name}"
    message: "Running {self.step_name} implementation: {self.implementation_name}" """
            + self._build_input()
            + f"""        
    output: {output_files}
    log: "{self.diagnostics_dir}/{self.name}-output{log_path_chunk_adder}.log"
    container: "{self.image_path}" """
        )
        return io_str

    def _build_input(self) -> str:
        input_str = f"""
    input:"""
        input_slots_to_split = self.get_input_slots_to_split(self.input_slots)
        for slot, attrs in self.input_slots.items():
            env_var = attrs["env_var"].lower()
            if len(input_slots_to_split) > 1:
                raise NotImplementedError(
                    "FIXME [MIC-5883] Multiple input slots to split not yet supported"
                )
            if self.is_embarrassingly_parallel and slot == input_slots_to_split[0]:
                # The input to this is the input_chunks subdir from the checkpoint
                # rule (which is built by modifying the output of the overall implementation)
                if len(self.output) > 1:
                    raise NotImplementedError(
                        "FIXME [MIC-5883] Multiple output slots/files of EmbarrassinglyParallelSteps not yet supported"
                    )
                input_files = [
                    os.path.dirname(self.output[0])
                    + "/input_chunks/{chunk}/"
                    + os.path.basename(self.output[0])
                ]
            else:
                input_files = attrs["filepaths"]
            input_str += f"""
        {env_var}={input_files},"""
        if not self.is_embarrassingly_parallel:
            # validations were already handled in the checkpoint rule - no need
            # to validate the individual chunks
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
        if self.is_embarrassingly_parallel:
            if len(self.output) > 1:
                raise NotImplementedError(
                    "FIXME [MIC-5883] Multiple output slots/files of EmbarrassinglyParallelSteps not yet supported"
                )
            output_files = (
                os.path.dirname(self.output[0])
                + "/processed/{wildcards.chunk}/"
                + os.path.basename(self.output[0])
            )
        else:
            output_files = ",".join(self.output)
        shell_cmd = f"""
    shell:
        '''
        export DUMMY_CONTAINER_OUTPUT_PATHS={output_files}
        export DUMMY_CONTAINER_DIAGNOSTICS_DIRECTORY={self.diagnostics_dir}"""
        for input_slot_name, input_slot_attrs in self.input_slots.items():
            input_slots_to_split = self.get_input_slots_to_split(self.input_slots)
            if len(input_slots_to_split) > 1:
                raise NotImplementedError(
                    "FIXME [MIC-5883] Multiple input slots to split not yet supported"
                )
            if input_slot_name in input_slots_to_split:
                # The inputs to this come from the input_chunks subdir
                input_files = (
                    os.path.dirname(self.output[0])
                    + "/input_chunks/{wildcards.chunk}/"
                    + os.path.basename(self.output[0])
                )
            else:
                input_files = ",".join(input_slot_attrs["filepaths"])
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
    validator: Callable
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

    When running an :class:`~easylink.implementation.Implementation` in an embarrassingly
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
    input_slots: dict[str, dict[str, str | list[str]]]
    """This ``Implementation's`` input slot attributes."""
    validations: list[str]
    """Validation files from previous rule."""
    output: list[str]
    """Output directory path. It must be used as an input for next rule."""

    def build_rule(self) -> str:
        """Builds the Snakemake rule for this checkpoint.

        Checkpoint rules are a special type of rule in Snakemake that allow for dynamic
        generation of output files. This rule is responsible for splitting the input
        files into chunks. Note that the output of this rule is a Snakemake ``directory``
        object as opposed to a specific file like typical rules have.
        """
        # Replace the output filepath with an input_chunks subdir
        output_dir = os.path.dirname(self.output[0]) + "/input_chunks"
        input_slots_to_split = self.get_input_slots_to_split(self.input_slots)
        if len(input_slots_to_split) > 1:
            raise NotImplementedError(
                "FIXME [MIC-5883] Multiple input slots to split not yet supported"
            )
        input_slot_to_split = input_slots_to_split[0]
        checkpoint = f"""
checkpoint:
    name: "split_{self.name}_{input_slot_to_split}"
    input: 
        files={self.input_slots[input_slot_to_split]['filepaths']},
        validations={self.validations},
    output: 
        output_dir=directory("{output_dir}"),
        checkpoint_file=touch("{output_dir}/checkpoint.txt"),
    params:
        input_files=lambda wildcards, input: ",".join(input.files),
    localrule: True
    message: "Splitting {self.name} {input_slot_to_split} into chunks"
    run:
        splitter_utils.{self.input_slots[input_slot_to_split]["splitter"].__name__}(
            input_files=list(input.files),
            output_dir=output.output_dir,
            desired_chunk_size_mb=0.1,
        )"""
        return checkpoint


@dataclass
class AggregationRule(Rule):
    """A :class:`Rule` that aggregates the processed chunks of output data.

    When running an :class:`~easylink.implementation.Implementation` in an embarrassingly
    parallel way, we need to aggregate the output files from each parallel job
    into a single output file.
    """

    name: str
    """Name of the rule."""
    input_slots: dict[str, dict[str, str | list[str]]]
    """This ``Implementation's`` input slot attributes."""
    output_slot_name: str
    """Name of the :class:`~easylink.graph_components.OutputSlot`."""
    output_slot: dict[str, str | list[str]]
    """The output slot attributes to create this rule for."""

    def build_rule(self) -> str:
        """Builds the Snakemake rule for this aggregator.

        When running an :class:`~easylink.step.EmbarrassinglyParallelStep`, we need
        to aggregate the output files from each parallel job into a single output file.
        This rule relies on a dynamically generated aggregation function which returns
        all of the **processed** chunks (from running the ``EmbarrassinglyParallelStep's``
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
        if len(self.output_slot["filepaths"]) > 1:
            raise NotImplementedError(
                "FIXME [MIC-5883] Multiple output slots/files of EmbarrassinglyParallelSteps not yet supported"
            )
        if len(self.output_slot["filepaths"]) > 1:
            raise NotImplementedError(
                "FIXME [MIC-5883] Multiple slots/files of EmbarrassinglyParallelSteps not yet supported"
            )
        output_filepath = self.output_slot["filepaths"][0]
        checkpoint_file_path = (
            os.path.dirname(output_filepath) + "/input_chunks/checkpoint.txt"
        )
        input_slots_to_split = self.get_input_slots_to_split(self.input_slots)
        if len(input_slots_to_split) > 1:
            raise NotImplementedError(
                "FIXME [MIC-5883] Multiple input slots to split not yet supported"
            )
        input_slot_to_split = input_slots_to_split[0]
        checkpoint_name = f"checkpoints.split_{self.name}_{input_slot_to_split}"
        output_files = (
            os.path.dirname(output_filepath)
            + "/processed/{chunk}/"
            + os.path.basename(output_filepath)
        )
        func = f"""
def get_aggregation_inputs_{self.name}_{self.output_slot_name}(wildcards):
    checkpoint_file = "{checkpoint_file_path}"
    if not os.path.exists(checkpoint_file):
        output, _ = {checkpoint_name}.rule.expand_output(wildcards)
        raise IncompleteCheckpointException({checkpoint_name}.rule, checkpoint_target(output[0]))
    checkpoint_output = glob.glob(f"{{{checkpoint_name}.get(**wildcards).output.output_dir}}/*/")
    chunks = [Path(filepath).parts[-1] for filepath in checkpoint_output]
    return expand(
        "{output_files}",
        chunk=chunks
    )"""
        return func

    def _define_aggregator_rule(self):
        """Builds the rule that runs the aggregation."""
        rule = f"""
rule:
    name: "aggregate_{self.name}_{self.output_slot_name}"
    input: get_aggregation_inputs_{self.name}_{self.output_slot_name}
    output: {self.output_slot["filepaths"]}
    localrule: True
    message: "Aggregating {self.name} {self.output_slot_name}"
    run:
        aggregator_utils.{self.output_slot["aggregator"].__name__}(
            input_files=list(input),
            output_filepath="{self.output_slot["filepaths"][0]}",
        )"""
        return rule
