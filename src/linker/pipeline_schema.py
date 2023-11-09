from typing import List

from linker.pipeline import Pipeline
from linker.step import Step


class PipelineSchema:
    """Defines the allowable schema for the pipeline and maintains the
    pipeline validation methods.
    """

    def __init__(self, name) -> None:
        self.name = name
        self.steps = []

    @classmethod
    def get_schemas(cls) -> List["PipelineSchema"]:
        """Creates the allowable schema for the pipeline."""
        schemas = []
        pvs_like_case_study = cls("pvs_like_case_study")
        pvs_like_case_study.add_step(Step("pvs_like_case_study"))
        schemas.append(pvs_like_case_study)
        return schemas

    def add_step(self, step: Step):
        self.steps.append(step)

    def validate_pipeline(self, pipeline: Pipeline):
        # TODO:
        ## ensure implementation is assigned to the correct step
        ## ensure that the implementations are in the correct step order
        return True


_PIPELINE_SCHEMAS = PipelineSchema.get_schemas()


def validate_pipeline(pipeline: Pipeline):
    """Validates the pipeline against supported schemas."""
    for schema in _PIPELINE_SCHEMAS:
        if schema.validate_pipeline(pipeline):
            return
    raise RuntimeError("Pipeline is not valid.")
