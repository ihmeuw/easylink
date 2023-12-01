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

    def run(self, runner: Callable, results_dir: Path) -> None:
        for implementation in self.implementations:
            implementation.run(
                runner=runner,
                container_engine=self.config.container_engine,
                input_data=self.config.input_data,
                results_dir=results_dir,
            )

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
            is_validated = False
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
            if not logs:
                is_validated = True
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
