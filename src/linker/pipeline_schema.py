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

        # pvs-like case study
        schemas.append(
            PipelineSchema._generate_schema(
                "pvs_like_case_study",
                # TODO: Make a real validator for
                # pvs_like_case_study and/or remove this hack
                lambda x: None,
                Step("pvs_like_case_study", validate_dummy_output),
            )
        )

        # development dummy
        schemas.append(
            PipelineSchema._generate_schema(
                "development",
                validate_dummy_input,
                Step(
                    "step_1",
                    validate_dummy_output,
                    input_slots={
                        "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS": {
                            "dir_name": "/input_data/main_input",
                            "filepaths": [
                                "/mnt/team/simulation_science/priv/engineering/er_ecosystem/sample_data/dummy/input_file_1.parquet"
                            ],
                            "prev_output": False,
                        }
                    },
                ),
                Step(
                    "step_2",
                    validate_dummy_output,
                    input_slots={
                        "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS": {
                            "dir_name": "/input_data/main_input",
                            "filepaths": [],
                            "prev_output": True,
                        },
                        "DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS": {
                            "dir_name": "/input_data/secondary_input",
                            "filepaths": [
                                "/mnt/team/simulation_science/priv/engineering/er_ecosystem/sample_data/dummy/input_file_1.parquet"
                            ],
                            "prev_output": False,
                        },
                    },
                ),
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
