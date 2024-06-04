from pathlib import Path
from typing import Callable, List, Optional

from easylink.step import Step
from easylink.utilities.validation_utils import validate_input_file_dummy


class PipelineSchema:
    """Defines the allowable schema(s) for the pipeline."""

    def __init__(self, name: str, validate_input: Callable) -> None:
        self.name = name
        self.validate_input = validate_input
        self.steps = []

    def __repr__(self) -> str:
        return f"PipelineSchema.{self.name}"

    @classmethod
    def _get_schemas(cls) -> List["PipelineSchema"]:
        """Creates the allowable schema for the pipeline."""
        schemas = []

        # pvs-like case study
        schemas.append(
            PipelineSchema._generate_schema(
                "pvs_like_case_study",
                # TODO: Make a real validator for
                # pvs_like_case_study and/or remove this hack
                lambda x: None,
                Step("pvs_like_case_study", prev_input=False, input_files=[]),
            )
        )

        # development dummy
        schemas.append(
            PipelineSchema._generate_schema(
                "development",
                validate_dummy_input,
                Step("step_1", prev_input=False, input_files=True),
                Step("step_2", prev_input=True, input_files=False),
                Step("step_3", prev_input=True, input_files=False),
                Step("step_4", prev_input=True, input_files=False),
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

    def __len__(self) -> int:
        # Later this might be the number of leaf nodes
        return len(self.steps)

    def get_step_id(self, step: Step) -> str:
        idx = self.steps.index(step)
        step_number = str(idx + 1).zfill(len(str(len(self))))
        return f"{step_number}_{step.name}"


def validate_dummy_input(filepath: Path) -> Optional[List[str]]:
    "Wrap the output file validator for now, since it is the same"
    try:
        validate_input_file_dummy(filepath)
    except Exception as e:
        return [e.args[0]]


PIPELINE_SCHEMAS = PipelineSchema._get_schemas()
