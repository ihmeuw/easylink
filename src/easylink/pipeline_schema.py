"""
===============
Pipeline Schema
===============

This module is responsible for managing the "pipeline schemas", i.e. the allowable 
and fully supported pipelines.

"""

from collections.abc import Iterable
from pathlib import Path

from layered_config_tree import LayeredConfigTree

from easylink.graph_components import EdgeParams, ImplementationGraph
from easylink.pipeline_schema_constants import SCHEMA_PARAMS
from easylink.step import HierarchicalStep, NonLeafConfigurationState, Step


class PipelineSchema(HierarchicalStep):
    """All possible pipelines that are fully supported.

    A ``PipelineSchema`` is a :class:`~easylink.step.HierarchicalStep` whose
    :class:`~easylink.graph_components.StepGraph` determines all possible allowable
    pipelines. The fundamental purpose of this class is to validate that the user-requested
    pipeline to run conforms to a fully supported pipeline.

    See :class:`~easylink.step.HierarchicalStep` for inherited attributes.

    Parameters
    ----------
    name
        The name of the pipeline schema.
    nodes
        The nodes of the pipeline schema.
    edges
        The edges of the pipeline schema.

    Notes
    -----
    A ``PipelineSchema`` is intended to be constructed by the :meth:`get_schema`
    class method.

    The ``PipelineSchema`` is a high-level abstraction; it represents the desired
    pipeline of conceptual steps to run with no detail as to how each of those
    steps is implemented.

    """

    def __init__(self, name: str, nodes: Iterable[Step], edges: Iterable[EdgeParams]) -> None:
        super().__init__(name, nodes=nodes, edges=edges)

    def __repr__(self) -> str:
        return f"PipelineSchema.{self.name}"

    def get_implementation_graph(self) -> ImplementationGraph:
        """Gets the :class:`~easylink.graph_components.ImplementationGraph`.

        The ``PipelineSchema`` is by definition a :class:`~easylink.step.HierarchicalStep`
        which has a :class:`~easylink.graph_components.StepGraph` containing
        sub-:class:`Steps<easylink.step.Step>` that need to be unrolled. This method
        recursively traverses that ``StepGraph`` and its childrens' ``StepGraphs``
        until all sub-``Steps`` are in a :class:`~easylink.step.LeafConfigurationState`,
        i.e. all ``Steps`` are implemented by a single ``Implementation`` and we
        have the desired ``ImplementationGraph``.

        Returns
        -------
            The ``ImplementationGraph`` of this ``PipelineSchema``.
        """
        implementation_graph = ImplementationGraph()
        self.add_nodes_to_implementation_graph(implementation_graph)
        self.add_edges_to_implementation_graph(implementation_graph)
        return implementation_graph

    def validate_step(
        self, pipeline_config: LayeredConfigTree, input_data_config: LayeredConfigTree
    ) -> dict[str, list[str]]:
        """Validates the pipeline configuration against this ``PipelineSchema``.

        Parameters
        ----------
        pipeline_config
            The pipeline configuration to validate.
        input_data_config
            The input data configuration.

        Returns
        -------
            A dictionary of errors, where the keys are the names of any steps that
            did not validate and the values are lists of as many error messages as
            could be generated for each of those steps.

        Notes
        -----
        Below, we nest the full pipeline configuration under a "substeps" key of
        a root :class:`~easylink.step.HierarchicalStep` because such a root step
        doesn't exist from the user's perspective and doesn't appear explicitly in
        the user-provided pipeline specification file.
        """
        return super().validate_step(
            LayeredConfigTree({"substeps": pipeline_config.steps.to_dict()}),
            pipeline_config.combined_implementations,
            input_data_config,
        )

    def validate_inputs(self, input_data: dict[str, Path]) -> dict[str, list[str]]:
        """Validates the file's existence and properties for each file slot.

        Parameters
        ----------
        input_data
            A dictionary mapping input data slot names to file paths.

        Returns
        -------
            A dictionary of errors, where the keys are the names of any files that
            did not validate and the values are lists of as many error messages as
            could be generated for each of those files.
        """
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

    def configure_pipeline(
        self, pipeline_config: LayeredConfigTree, input_data_config: LayeredConfigTree
    ) -> None:
        """Configures the ``PipelineSchema`` and corresponding `StepGraphs<easylink.graph_components.StepGraph`.

        The configuration state of any :class:`~easylink.step.Step` tells whether
        that ``Step`` is a leaf or a non-leaf node and is assigned to the
        :attr:`easylink.step.Step.configuration_state`. By definition, the entire
        ``PipelineSchema`` has non-leaf configuration state; this method thus assigns
        a :class:`~easylink.step.NonLeafConfigurationState` to the ``PipelineSchema``.
        Upon instantiation, this ``NonLeafConfigurationState`` recursively updates
        the ``StepGraphs`` until all non-leaf nodes are resolved.

        Parameters
        ----------
        pipeline_config
            The pipeline configuration.
        input_data_config
            The input data configuration.
        """
        self._configuration_state = NonLeafConfigurationState(
            self,
            pipeline_config.steps,
            combined_implementations=pipeline_config.combined_implementations,
            input_data_config=input_data_config,
        )

    @classmethod
    def get_schema(cls, name: str = "main") -> list["PipelineSchema"]:
        """Gets all allowable ``PipelineSchemas``.

        These ``PipelineSchemas`` represent the fully supported pipelines and are
        used to validate the user-requested pipeline.

        Parameters
        ----------
        name
            The name of the ``PipelineSchema`` to get.

        Returns
        -------
            The requested ``PipelineSchema``.
        """
        if name not in SCHEMA_PARAMS:
            raise ValueError(f"Pipeline schema '{name}' is not supported.")
        return cls(name, *SCHEMA_PARAMS[name])
