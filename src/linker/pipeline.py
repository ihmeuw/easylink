from collections import defaultdict
from pathlib import Path
from typing import Dict, Tuple

import yaml

from linker.configuration import Config
from linker.implementation import Implementation
from linker.rule import ImplementedRule, TargetRule, ValidationRule
from linker.utilities.general_utils import exit_with_validation_error


class Pipeline:
    """Abstraction to handle pipeline specification and execution."""

    def __init__(self, config: Config):
        self.config = config
        self.implementations = self._get_implementations()
        # TODO [MIC-4880]: refactor into validation object
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

    ### Say Why this needs to be in pipeline and not implementation
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
        self.write_imports(results_dir)
        self.write_target_rules(results_dir)
        for implementation in self.implementations:
            self.write_implementation_rules(implementation, results_dir)
        return results_dir / "Snakefile"

    def write_imports(self, results_dir) -> None:
        snakefile = results_dir / "Snakefile"
        with open(snakefile, "a") as f:
            f.write("from linker.utilities.validation_utils import *")

    def write_target_rules(self, results_dir) -> None:
        final_output = [str(results_dir / "result.parquet")]
        target_rule = TargetRule(target_files=final_output, validation="final_validator")
        final_validation = ValidationRule(
            name="validate_results",
            input=final_output,
            output="final_validator",
            validator="validate_dummy_file",
        )
        target_rule.write_to_snakefile(results_dir)
        final_validation.write_to_snakefile(results_dir)

    def write_implementation_rules(self, implementation, results_dir) -> None:
        input_files = self.get_input_files(implementation.name, results_dir)
        output_files = [
            str(self.get_output_dir(implementation.name, results_dir) / "result.parquet"),
        ]
        validation_file = str(results_dir / implementation.validation_filename)
        validation_rule = ValidationRule(
            name=implementation.name,
            input=input_files,
            output=validation_file,
            validator=implementation.step.validate_file,
        )
        implementation_rule = ImplementedRule(
            name=implementation.name,
            execution_input=input_files,
            validation=validation_file,
            output=output_files,
            script_path=implementation.script(),
        )
        validation_rule.write_to_snakefile(results_dir)
        implementation_rule.write_to_snakefile(results_dir)
