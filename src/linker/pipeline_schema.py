from typing import Callable, List

from linker.pipeline_schema_constants import ALLOWED_SCHEMA_PARAMS
from linker.step import Step


class PipelineSchema:
    """Defines the allowable schema(s) for the pipeline."""

    def __init__(self, name, validate_input) -> None:
        self.name = name
        self.validate_input: Callable = validate_input
        self.steps = []

    def __repr__(self) -> str:
        return f"PipelineSchema.{self.name}"

    @classmethod
    def _get_schemas(cls) -> List["PipelineSchema"]:
        """Creates the allowable schema for the pipeline."""
        schemas = []
        for schema_name, schema_params in ALLOWED_SCHEMA_PARAMS.items():
            schema = cls(schema_name, schema_params["validate_input"])
            for step_name, step_params in schema_params["steps"].items():
                schema.steps.append(Step(step_name, **step_params))
            schemas.append(schema)

        return schemas


PIPELINE_SCHEMAS = PipelineSchema._get_schemas()
