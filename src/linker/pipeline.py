from collections import defaultdict
from pathlib import Path
from typing import Callable, Dict, Optional, Tuple

import yaml

from linker.configuration import Config
from linker.implementation import Implementation
from linker.rule import ImplementedRule, ValidationRule
from linker.utilities.general_utils import exit_with_validation_error


class Pipeline:
    """Abstraction to handle pipeline specification and execution."""

    def __init__(self, config: Config):
        self.config = config
        self.implementations = self._get_implementations()
        self._validate()

    def _get_implementations(self) -> Tuple[Implementation, ...]:
        return tuple(
            Implementation(
                config=self.config,
                step=step,
            )
            for step in self.config.schema.steps
        )

    def _validate(self) -> None:
        """Validates the pipeline."""

        # TODO: validate that spark and slurm resources are requested if needed
        errors = {**self._validate_implementations()}

        if errors:
            exit_with_validation_error(errors)

    def _validate_implementations(self) -> Dict:
        """Validates each individual Implementation instance."""
        errors = defaultdict(dict)
        for implementation in self.implementations:
            implementation_errors = implementation.validate()
            if implementation_errors:
                errors["IMPLEMENTATION ERRORS"][implementation.name] = implementation_errors
        return errors

    @property
    def implementation_indices(self) -> dict:
        return {
            implementation.name: idx
            for idx, implementation in enumerate(self.implementations)
        }

    def get_input_files(self, implementation_name: str, results_dir: Path) -> list:
        idx = self.implementation_indices[implementation_name]
        if idx == 0:
            return [str(file) for file in self.config.input_data]
        else:
            previous_output_dir = self.get_output_dir(
                self.implementations[idx - 1].name, results_dir
            )
            return [str(previous_output_dir / "result.parquet")]

    def get_output_dir(self, implementation_name: str, results_dir: Path) -> Path:
        idx = self.implementation_indices[implementation_name]
        num_steps = len(self.implementations)
        step_number = str(idx + 1).zfill(len(str(len(self.implementations))))
        if idx == num_steps - 1:
            return results_dir

        return (
            results_dir
            / "intermediate"
            / f"{step_number}_{self.implementations[idx].step_name}"
        )

    def build_snakefile(self, results_dir: Path) -> None:
        self.write_rule_all(results_dir)
        for implementation in self.implementations:
            self.write_rule(implementation, results_dir)
        return results_dir / "Snakefile"

    def write_rule_all(self, results_dir):
        snakefile = results_dir / "Snakefile"
        validation_files = [
            (results_dir / implementation.validation_filename).as_posix()
            for implementation in self.implementations
        ]
        with open(snakefile, "a") as f:
            f.write(
                f"""
from linker.utilities.validation_utils import *"""
            )
            f.write(
                f"""
rule all:
    input:
        final_output="{results_dir}/result.parquet",
        validations={validation_files}
                """
            )

    def write_rule(self, implementation, results_dir):
        input_files = self.get_input_files(implementation.name, results_dir)
        output_files = [
            str(self.get_output_dir(implementation.name, results_dir) / "result.parquet")
        ]
        rule = ImplementedRule(
            implementation.name,
            input_files,
            output_files,
            implementation.script(),
        )
        validation = ValidationRule(
            implementation.name,
            output_files,
            (results_dir / implementation.validation_filename).as_posix(),
            validator="validate_dummy_file",
        )
        rule.write_to_snakefile(results_dir)
        validation.write_to_snakefile(results_dir)
