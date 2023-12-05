from typing import List

from linker.step import Step


class PipelineSchema:
    """Defines the allowable schema for the pipeline and maintains the
    pipeline validation methods.
    """

    def __init__(self, name) -> None:
        self.name = name
        self.steps = []

    def __repr__(self) -> str:
        return f"PipelineSchema.{self.name}"

    @classmethod
    def _get_schemas(cls) -> List["PipelineSchema"]:
        """Creates the allowable schema for the pipeline."""
        schemas = []

        # pvs-like case study
        pvs_like_case_study = cls("pvs_like_case_study")
        pvs_like_case_study._add_step(Step("pvs_like_case_study"))
        schemas.append(pvs_like_case_study)

        # development dummy
        development = cls("development")
        development._add_step(Step("step_1"))
        development._add_step(Step("step_2"))
        schemas.append(development)

        return schemas

    def _add_step(self, step: Step) -> None:
        self.steps.append(step)


PIPELINE_SCHEMAS = PipelineSchema._get_schemas()
