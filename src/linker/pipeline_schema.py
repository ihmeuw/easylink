from pathlib import Path
from typing import Callable, List, Optional

from linker.step import Step
from linker.utilities.data_utils import validate_dummy_output


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

        # development dummy
        schemas.append(
            PipelineSchema._generate_schema(
                "development",
                validate_dummy_input,
                Step("step_1"),
                Step("step_2"),
            )
        )

        return schemas

    def _add_step(self, step: Step) -> None:
        self.steps.append(step)

    @classmethod
    def _generate_schema(cls, name: str, validate_input: Callable, *steps: Step) -> None:
        schema = cls(name, validate_input)
        for step in steps:
            schema._add_step(step)
        return schema


def validate_dummy_input(filepath: Path) -> Optional[List[str]]:
    "Wrap the output file validator for now, since it is the same"
    try:
        validate_dummy_output(filepath)
    except Exception as e:
        return [e.args[0]]


PIPELINE_SCHEMAS = PipelineSchema._get_schemas()
