import itertools
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Tuple

import networkx as nx
from loguru import logger
from networkx import MultiDiGraph

from easylink.configuration import Config
from easylink.implementation import Implementation
from easylink.rule import ImplementedRule, InputValidationRule, TargetRule
from easylink.utilities.general_utils import exit_with_validation_error
from easylink.utilities.paths import SPARK_SNAKEFILE
from easylink.utilities.validation_utils import validate_input_file_dummy


class Pipeline:
    """Abstraction to handle pipeline specification and execution."""

    def __init__(self, config: Config):
        self.config = config
        self.pipeline_graph = self._get_pipeline_graph(config)
        # TODO [MIC-4880]: refactor into validation object
        self._validate()

    def _get_pipeline_graph(self, config) -> MultiDiGraph:
        graph = MultiDiGraph()
        graph.add_node("input_data")
        prev_nodes = None

        for step in config.schema.steps:
            step_graph = step.get_subgraph(config)
            step_source_nodes = [node for node, deg in step_graph.in_degree() if deg == 0]
            graph = nx.compose(graph, step_graph)
            for source_node in step_source_nodes:
                # Connect new source nodes to old sink nodes
                if step.prev_input:
                    for prev_node in prev_nodes:
                        graph.add_edge(
                            prev_node,
                            source_node,
                            files=[str(Path("intermediate") / prev_node / "result.parquet")],
                        )
                # Add input data to the first step
                # This will probably need to be a node attribute in #TODO: [MIC-4774]
                if step.input_files:
                    graph.add_edge(
                        "input_data",
                        source_node,
                        files=[str(file) for file in config.input_data],
                    )
            prev_nodes = [node for node, deg in graph.out_degree() if deg == 0]
        graph.add_node("results")

        for node in prev_nodes:
            graph.add_edge(node, "results", files=["result.parquet"])
        return graph

    @property
    def implementation_nodes(self) -> List[str]:
        ordered_nodes = list(nx.topological_sort(self.pipeline_graph))
        return [node for node in ordered_nodes if node != "input_data" and node != "results"]

    def _validate(self) -> None:
        """Validates the pipeline."""

        errors = {**self._validate_implementations()}

        if errors:
            exit_with_validation_error(errors)

    def _validate_implementations(self) -> Dict:
        """Validates each individual Implementation instance."""
        errors = defaultdict(dict)
        for implementation in nx.get_node_attributes(
            self.pipeline_graph, "implementation"
        ).values():
            implementation_errors = implementation.validate()
            if implementation_errors:
                errors["IMPLEMENTATION ERRORS"][implementation.name] = implementation_errors
        return errors

    @property
    def snakefile_path(self) -> Path:
        return self.config.results_dir / "Snakefile"

    def build_snakefile(self) -> Path:
        if self.snakefile_path.is_file():
            logger.warning("Snakefile already exists, overwriting.")
            self.snakefile_path.unlink()
        self.write_imports()
        self.write_config()
        self.write_target_rules()
        if self.config.spark:
            self.write_spark_module()
        for node in self.implementation_nodes:
            self.write_implementation_rules(node)
        return self.snakefile_path

    def write_imports(self) -> None:
        with open(self.snakefile_path, "a") as f:
            f.write("from easylink.utilities import validation_utils")

    def write_target_rules(self) -> None:
        final_output = ["result.parquet"]
        validator_file = str("input_validations/final_validator")
        # Snakemake resolves the DAG based on the first rule, so we put the target
        # before the validation
        target_rule = TargetRule(
            target_files=final_output,
            validation=validator_file,
            requires_spark=bool(self.config.spark),
        )
        final_validation = InputValidationRule(
            name="results",
            input=final_output,
            output=validator_file,
            validator=validate_input_file_dummy,
        )
        target_rule.write_to_snakefile(self.snakefile_path)
        final_validation.write_to_snakefile(self.snakefile_path)

    def write_implementation_rules(self, node: str) -> None:
        implementation = self.pipeline_graph.nodes[node]["implementation"]
        input_files, output_files = self.get_input_output_files(node)
        diagnostics_dir = Path("diagnostics") / node
        diagnostics_dir.mkdir(parents=True, exist_ok=True)
        validation_file = f"input_validations/{implementation.validation_filename}"
        resources = (
            self.config.slurm_resources
            if self.config.computing_environment == "slurm"
            else None
        )
        validation_rule = InputValidationRule(
            name=implementation.name,
            input=input_files,
            output=validation_file,
            validator=self.pipeline_graph.nodes[node]["input_validator"],
        )
        implementation_rule = ImplementedRule(
            step_name=implementation.schema_step_name,
            implementation_name=implementation.name,
            execution_input=input_files,
            validation=validation_file,
            output=output_files,
            resources=resources,
            envvars=implementation.environment_variables,
            diagnostics_dir=str(diagnostics_dir),
            image_path=implementation.singularity_image_path,
            script_cmd=implementation.script_cmd,
            requires_spark=implementation.requires_spark,
        )
        validation_rule.write_to_snakefile(self.snakefile_path)
        implementation_rule.write_to_snakefile(self.snakefile_path)

    def get_input_output_files(self, node: str) -> Tuple[List[str], List[str]]:
        input_files = list(
            itertools.chain.from_iterable(
                [
                    data["files"]
                    for _, _, data in self.pipeline_graph.in_edges(node, data=True)
                ]
            )
        )
        output_files = list(
            itertools.chain.from_iterable(
                [
                    data["files"]
                    for _, _, data in self.pipeline_graph.out_edges(node, data=True)
                ]
            )
        )
        return input_files, output_files

    def write_config(self) -> None:
        with open(self.snakefile_path, "a") as f:
            if self.config.spark:
                f.write(
                    f"\nscattergather:\n\tnum_workers={self.config.spark_resources['num_workers']},"
                )

    def write_spark_module(self) -> None:
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
        slurm_account={self.config.slurm_resources['slurm_account']},
        slurm_partition={self.config.slurm_resources['slurm_partition']},
        mem_mb={self.config.spark_resources['slurm_mem_mb']},
        runtime={self.config.spark_resources['runtime']},
        cpus_per_task={self.config.spark_resources['cpus_per_task']},
        slurm_extra="--output 'spark_logs/start_spark_master-slurm-%j.log'"
use rule start_spark_worker from spark_cluster with:
    resources:
        slurm_account={self.config.slurm_resources['slurm_account']},
        slurm_partition={self.config.slurm_resources['slurm_partition']},
        mem_mb={self.config.spark_resources['slurm_mem_mb']},
        runtime={self.config.spark_resources['runtime']},
        cpus_per_task={self.config.spark_resources['cpus_per_task']},
        slurm_extra="--output 'spark_logs/start_spark_worker-slurm-%j.log'"
    params:
        terminate_file_name=rules.terminate_spark.output,
        user=os.environ["USER"],
        cores={self.config.spark_resources['cpus_per_task']},
        memory={self.config.spark_resources['mem_mb']}
                        """
            f.write(module)
