from typing import List

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

    def _add_step(self, step: Step) -> None:
        self.steps.append(step)


PIPELINE_SCHEMAS = PipelineSchema.get_schemas()
