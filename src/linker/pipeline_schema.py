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
        # Add the steps in the order they should be run
        pvs_like_case_study._add_step(Step("pvs_like_case_study"))
        schemas.append(pvs_like_case_study)
        return schemas

    def _add_step(self, step: Step):
        self.steps.append(step)

    def validate_pipeline(self, pipeline: Pipeline):
        # Loop through the pipeline implementations and check if their order
        ## and corresponding steps match the schema
        for idx, implementation in enumerate(pipeline.implementations):
            if implementation.step != self.steps[idx]:
                return False
        return True


def validate_pipeline(pipeline: Pipeline):
    """Validates the pipeline against supported schemas."""
    for schema in PipelineSchema.get_schemas():
        if schema.validate_pipeline(pipeline):
            return
        else:  # invalid pipeline for this schema
            pass  # try the next schema
    raise RuntimeError("Pipeline is not valid.")
