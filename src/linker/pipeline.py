from collections import defaultdict
from pathlib import Path
from typing import Callable, Dict, Optional, Tuple

from loguru import logger

from linker.configuration import Config
from linker.implementation import Implementation
from linker.pipeline_schema import PIPELINE_SCHEMAS, PipelineSchema
from linker.step import Step


class Pipeline:
    """Abstraction to handle pipeline specification and execution."""

    def __init__(self, config: Config):
        self.config = config
        self.steps = self._get_steps()
        self.implementations = self._get_implementations()
        self.schema = None
        self._validate()

    def run(
        self,
        runner: Callable,
        results_dir: Path,
        session: Optional["drmaa.Session"],
    ) -> None:
        number_of_steps = len(self.implementations)
        number_of_steps_digit_length = len(str(number_of_steps))
        for idx, implementation in enumerate(self.implementations):
            step_number = str(idx + 1).zfill(number_of_steps_digit_length)
            previous_step_number = str(idx).zfill(number_of_steps_digit_length)
            step_id = f"{step_number}_{implementation.step_name}"
            diagnostics_dir = results_dir / "diagnostics" / step_id
            diagnostics_dir.mkdir(parents=True, exist_ok=True)
            output_dir = (
                results_dir
                if idx == (number_of_steps - 1)
                else results_dir
                / "intermediate"
                / f"{step_number}_{implementation.step_name}"
            )
            input_data = self.config.input_data
            if idx <= number_of_steps - 1:
                output_dir.mkdir(exist_ok=True)
            if idx > 0:
                # Overwrite the pipeline input data with the results of the previous step
                input_data = [
                    file
                    for file in (
                        output_dir
                        / "intermediate"
                        / f"{previous_step_number}_{self.implementations[idx - 1].step_name}"
                    ).glob("*.parquet")
                ]
            implementation.run(
                session=session,
                runner=runner,
                container_engine=self.config.container_engine,
                step_id=step_id,
                input_data=input_data,
                results_dir=output_dir,
                diagnostics_dir=diagnostics_dir,
            )
        # Close the drmaa session (if one exists) once the pipeline is finished
        if session:
            session.exit()

    #################
    # Setup methods #
    #################

    def _get_steps(self) -> Tuple[Step, ...]:
        return tuple(Step(step) for step in self.config.pipeline["steps"])

    def _get_implementations(self) -> Tuple[Implementation, ...]:
        resources = {key: self.config.environment.get(key) for key in ["slurm", "spark"]}
        return tuple(
            Implementation(
                step=step,
                implementation_name=self.config.get_implementation_name(step.name),
                implementation_config=self.config.get_implementation_config(step.name),
                container_engine=self.config.container_engine,
                resources=resources,
            )
            for step in self.steps
        )

    def _validate(self) -> None:
        import errno

        import yaml

        errors = {
            **self._validate_pipeline(),
            **self._validate_implementations(),
            **self._validate_input_data(),
        }
        if errors:
            yaml_str = yaml.dump(errors)
            logger.error(
                "\n\n=========================================="
                "\nValidation errors found. Please see below."
                f"\n\n{yaml_str}"
                "\nValidation errors found. Please see above."
                "\n==========================================\n"
            )
            exit(errno.EINVAL)

    def _validate_pipeline(self) -> Dict:
        """Validates the pipeline against supported schemas."""

        errors = defaultdict(dict)
        for schema in PIPELINE_SCHEMAS:
            logs = []
            # Check that number of schema steps matches number of implementations
            if len(schema.steps) != len(self.implementations):
                logs.append(
                    f"Expected {len(schema.steps)} steps but found {len(self.implementations)} implementations."
                )
            else:
                for idx, implementation in enumerate(self.implementations):
                    # Check that all steps are accounted for and in the correct order
                    schema_step = schema.steps[idx].name
                    pipeline_step = implementation._pipeline_step_name
                    if pipeline_step != schema_step:
                        logs.append(
                            f"Step {idx + 1}: the pipeline schema expects step '{schema_step}' "
                            f"but the provided pipeline specifies '{pipeline_step}'. "
                            "Check step order and spelling in the pipeline configuration yaml."
                        )
            if logs:
                errors["PIPELINE ERRORS"][schema.name] = logs
                pass  # try the next schema
            else:
                self.schema = schema
                return {}  # we have a winner
        # No schemas were validated
        return errors

    def _validate_implementations(self) -> Dict:
        """Validates each individual Implementation instance."""
        errors = defaultdict(dict)
        for implementation in self.implementations:
            implementation_errors = implementation.validate()
            if implementation_errors:
                errors["IMPLEMENTATION ERRORS"][implementation.name] = implementation_errors
        return errors

    def _validate_input_data(self) -> Dict:
        errors = defaultdict(dict)
        for input_filepath in self.config.input_data:
            input_data_errors = self.schema.validate_input(input_filepath) if self.schema else None
            if input_data_errors:
                errors["INPUT DATA ERRORS"][str(input_filepath)] = input_data_errors
        return errors
