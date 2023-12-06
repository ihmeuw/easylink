import types
from pathlib import Path
from typing import Callable, List, Optional, Tuple

from loguru import logger

from linker.configuration import Config
from linker.implementation import Implementation
from linker.step import Step


class Pipeline:
    """Abstraction to handle pipeline specification and execution."""

    def __init__(self, config: Config):
        self.config = config
        self.steps = self._get_steps()
        self.implementations = self._get_implementations()
        self._validation_errors = {}
        self._validate()

    def run(
        self,
        runner: Callable,
        results_dir: Path,
        log_dir: Path,
        session: Optional[types.ModuleType("drmaa.Session")],
    ) -> None:
        number_of_steps = len(self.implementations)
        for idx, implementation in enumerate(self.implementations):
            output_dir = (
                results_dir
                if idx == (number_of_steps - 1)
                else results_dir / "intermediate" / implementation.name
            )
            if idx == 0:
                # Run the first step (which requires the input data and which
                # writes out to the results intermediate directory)
                output_dir.mkdir(exist_ok=True)
                input_data = self.config.input_data
            elif idx == number_of_steps - 1:
                # Run the last step (which requires the results of the previous step
                # and which writes out to the results parent directory)
                input_data = [
                    f
                    for f in (
                        output_dir / "intermediate" / self.implementations[idx - 1].name
                    ).glob("*.parquet")
                ]
            else:
                # Run the middle steps (which require the results of the previous
                # step and which write out to the results intermediate directory)
                output_dir.mkdir(exist_ok=True)
                input_data = [
                    f
                    for f in (
                        output_dir / "intermediate" / self.implementations[idx - 1].name
                    ).glob("*.parquet")
                ]
            implementation.run(
                runner=runner,
                container_engine=self.config.container_engine,
                input_data=input_data,
                results_dir=output_dir,
                log_dir=log_dir,
            )
        if session:
            session.exit()

    #################
    # Setup methods #
    #################

    def _get_steps(self) -> Tuple[Step, ...]:
        return tuple(Step(step) for step in self.config.pipeline["steps"])

    def _get_implementations(self) -> Tuple[Implementation, ...]:
        return tuple(
            Implementation(
                step.name,
                self.config.pipeline["steps"][step.name]["implementation"],
                self.config.container_engine,
            )
            for step in self.steps
        )

    def _validate(self) -> None:
        import errno

        import yaml

        validations = []
        for validation in [
            self._validate_pipeline,
            self._validate_implementations,
        ]:
            validations.append(validation())
        if not all(validations):
            yaml_str = yaml.dump(self._validation_errors)
            logger.error(
                "\n\n=========================================="
                "\nValidation errors found. Please see below."
                f"\n\n{yaml_str}"
                "\nValidation errors found. Please see above."
                "\n==========================================\n"
            )
            exit(errno.EINVAL)

    def _validate_pipeline(self) -> bool:
        """Validates the pipeline against supported schemas."""

        from linker.pipeline_schema import PIPELINE_SCHEMAS, PipelineSchema

        def _validate(schema: PipelineSchema) -> Tuple[bool, List[Optional[str]]]:
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
            is_validated = False if logs else True
            return is_validated, logs

        errors = {}
        for schema in PIPELINE_SCHEMAS:
            is_validated, schema_errors = _validate(schema)
            if not is_validated:
                errors[schema.name] = schema_errors
                pass  # try the next schema
            else:
                return True  # we have a winner
        # No schemas were validated
        self._validation_errors["PIPELINE ERRORS"] = errors
        return False

    def _validate_implementations(self) -> bool:
        """Validates each individual Implementation instance."""
        errors = {}
        for implementation in self.implementations:
            implementation_errors = implementation.validate()
            if implementation_errors:
                errors[implementation.name] = implementation_errors
        if errors:
            self._validation_errors["IMPLEMENTATION ERRORS"] = errors
            return False
        else:
            return True
