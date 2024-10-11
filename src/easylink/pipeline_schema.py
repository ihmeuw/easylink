from pathlib import Path
from typing import Iterable

from layered_config_tree import LayeredConfigTree

from easylink.graph_components import EdgeParams
from easylink.pipeline_schema_constants import ALLOWED_SCHEMA_PARAMS
from easylink.step import CompositeState, HierarchicalStep, Step


class PipelineSchema(HierarchicalStep):
    """
    A schema is a Step whose StephGraph determines all possible
    allowable pipelines.
    """

    def __init__(self, name: str, nodes: Iterable[Step], edges: Iterable[EdgeParams]) -> None:
        super().__init__(name, nodes=nodes, edges=edges)

    def __repr__(self) -> str:
        return f"PipelineSchema.{self.name}"

    def get_state_config(self, parent_config: LayeredConfigTree) -> None:
        self._config = parent_config

    def validate_step(
        self, pipeline_config: LayeredConfigTree, input_data_config: LayeredConfigTree
    ) -> dict[str, list[str]]:
        return super().validate_step({"substeps": pipeline_config}, input_data_config)

    def configure_step(
        self, parent_config: LayeredConfigTree, input_data_config: LayeredConfigTree
    ) -> None:
        self._layer_state = CompositeState(self, parent_config)
        self.layer_state.configure_subgraph_steps(input_data_config)

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
