from collections.abc import Iterable
from pathlib import Path

from layered_config_tree import LayeredConfigTree

from easylink.graph_components import EdgeParams
from easylink.pipeline_schema_constants import ALLOWED_SCHEMA_PARAMS
from easylink.step import HierarchicalStep, NonLeafConfigurationState, Step


class PipelineSchema(HierarchicalStep):
    """An abstraction of all possible allowable pipelines.

    A PipelineSchema is a Step whose StepGraph determines all possible allowable
    pipelines. The fundamental purpose of this class is to validate that the
    user-requested pipeline to run conforms to an allowable pipeline.
    """

    def __init__(self, name: str, nodes: Iterable[Step], edges: Iterable[EdgeParams]) -> None:
        super().__init__(name, nodes=nodes, edges=edges)

    def __repr__(self) -> str:
        return f"PipelineSchema.{self.name}"

    def validate_step(
        self, pipeline_config: LayeredConfigTree, input_data_config: LayeredConfigTree
    ) -> dict[str, list[str]]:
        """Validates the pipeline configuration against this instance of pipeline schema.

        Notes
        -----
        Below, we nest the full pipeline configuration under a "substeps" key of
        a root hierarchical step because the root step doesn't exist from the
        user's perspective and it doesn't appear explicitly in the pipeline.yaml.
        """
        return super().validate_step(
            step_config={"substeps": pipeline_config["steps"]},
            combined_implementations=pipeline_config["combined_implementations"],
            input_data_config=input_data_config,
        )

    def configure_pipeline(
        self, pipeline_config: LayeredConfigTree, input_data_config: LayeredConfigTree
    ) -> None:
        """Configures the PipelineSchema and corresponding StepGraphs.

        Notes
        -----
        This recursively updates the StepGraphs until all non-leaf nodes are resolved.
        """
        self._configuration_state = NonLeafConfigurationState(
            self,
            pipeline_config["steps"],
            combined_implementations=pipeline_config["combined_implementations"],
            input_data_config=input_data_config,
        )

    @classmethod
    def _get_schemas(cls) -> list["PipelineSchema"]:
        """Creates all allowable schemas for the pipeline."""
        return [
            cls(name, nodes=nodes, edges=edges)
            for name, (nodes, edges) in ALLOWED_SCHEMA_PARAMS.items()
        ]

    def validate_inputs(self, input_data: dict[str, Path]) -> dict[str, list[str]]:
        "Validates the file's existence and properties for each file slot."
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
