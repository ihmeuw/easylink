from collections import defaultdict
from pathlib import Path
from typing import Callable, List, Optional

from linker.pipeline_schema_constants import ALLOWED_SCHEMA_PARAMS
from linker.step import Step


class PipelineSchema:
    """Defines the allowable schema(s) for the pipeline."""

    def __init__(self, name, input_validator) -> None:
        self.name = name
        self._input_validator: Callable = input_validator
        self.steps: List[Step] = []

    def __repr__(self) -> str:
        return f"PipelineSchema.{self.name}"

    def validate_input(self, filepath: Path) -> Optional[List[str]]:
        return self._input_validator(filepath)

    @classmethod
    def _get_schemas(cls) -> List["PipelineSchema"]:
        """Creates the allowable schema for the pipeline."""
        schemas = []
        for schema_name, schema_params in ALLOWED_SCHEMA_PARAMS.items():
            schema = cls(schema_name, schema_params["input_validator"])
            for step_name, step_params in schema_params["steps"].items():
                schema.steps.append(Step(step_name, **step_params))
            schemas.append(schema)

        return schemas

    def add_input_filename_bindings(self, input_data: dict) -> None:
        for step in self.steps:
            step.add_input_filename_bindings(input_data)

    def validate_input_filenames(self, input_data: dict) -> dict:
        errors = defaultdict(dict)
        for step in self.steps:
            step_errors = step.validate_input_filenames(input_data)
            if step_errors:
                errors["STEP INPUT ERRORS"][step.name] = step_errors
        return errors


PIPELINE_SCHEMAS = PipelineSchema._get_schemas()
