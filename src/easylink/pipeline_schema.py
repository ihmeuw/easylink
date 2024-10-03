from pathlib import Path

from layered_config_tree import LayeredConfigTree

from easylink.pipeline_schema_constants import ALLOWED_SCHEMA_PARAMS
from easylink.step import Step


class PipelineSchema(Step):
    """
    A schema is a Step whose StephGraph determines all possible
    allowable pipelines.
    """

    def __repr__(self) -> str:
        return f"PipelineSchema.{self.name}"

    def is_composite(self, step_config) -> bool:
        return True

    def set_step_config(self, parent_config: LayeredConfigTree) -> None:
        self._config = parent_config

    @classmethod
    def _get_schemas(cls) -> list["PipelineSchema"]:
        """Creates the allowable schemas for the pipeline."""
        return [
            cls(name, nodes=nodes, edges=edges)
            for name, (nodes, edges) in ALLOWED_SCHEMA_PARAMS.items()
        ]

    def validate_inputs(self, input_data: dict[str, Path]) -> dict[str, list[str]]:
        "For each file slot used from the input data, validate the file's existence and properties."
        errors = {}
        for _, _, edge_attrs in self.step_graph.out_edges("input_data", data=True):
            validator = edge_attrs["input_slot"].validator
            for file in input_data.values():
                try:
                    validator(file)
                except FileNotFoundError as e:
                    errors[str(file)] = ["File not found."]
                except Exception as e:
                    errors[str(file)] = [e.args[0]]
        return errors


PIPELINE_SCHEMAS = PipelineSchema._get_schemas()
