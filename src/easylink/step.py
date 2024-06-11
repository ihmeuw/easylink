from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING

from networkx import MultiDiGraph

from easylink.implementation import Implementation

if TYPE_CHECKING:
    from easylink.configuration import Config


class Step(ABC):
    @abstractmethod
    def get_subgraph(self, config: "Config") -> MultiDiGraph:
        pass


class CompositeStep(Step):
    def __init__(self, name: str, **params) -> None:
        self.name = name
        self.out_dir = Path()
        self.graph = self._create_graph(params)

    def _create_graph(self, params: dict) -> MultiDiGraph:
        graph = MultiDiGraph()
        for step_name, graph_params in params.items():
            step = graph_params["step_type"](
                step_name,
                **graph_params["step_params"],
            )
            graph.add_node(
                step_name,
                step=step,
                out_dir=step.out_dir,
            )
            for in_edge, edge_params in graph_params.get("in_edges", {}).items():
                graph.add_edge(in_edge, step_name, **edge_params)

        return graph

    def get_subgraph(self, config: "Config") -> MultiDiGraph:
        curr_graph = MultiDiGraph(incoming_graph_data=self.graph)

        for subgraph_node in self.graph.nodes:
            subgraph = self.graph.nodes[subgraph_node]["step"].get_subgraph(config)
            curr_graph.update(subgraph)
            subgraph_source_nodes = [
                node for node in subgraph.nodes if subgraph.in_degree(node) == 0
            ]

            subgraph_sink_nodes = [
                node for node in subgraph.nodes if subgraph.out_degree(node) == 0
            ]
            curr_input_edges = [
                (source, subgraph_node, data)
                for source, _, data in curr_graph.in_edges(subgraph_node, data=True)
            ]

            curr_output_edges = [
                (subgraph_node, sink, data)
                for _, sink, data in curr_graph.out_edges(subgraph_node, data=True)
            ]

            for source, _, data in curr_input_edges:
                curr_graph.add_edges_from(
                    [(source, node, data) for node in subgraph_source_nodes]
                )

            # Get absolute paths from relative paths
            for _, sink, data in curr_output_edges:
                for sub_node in subgraph_sink_nodes:
                    data["files"] = [
                        str(curr_graph.nodes[sub_node]["out_dir"] / file)
                        for file in data["files"]
                    ]
                    curr_graph.add_edge(sub_node, sink, **data)
            # Remove the original node
            curr_graph.remove_node(subgraph_node)
        return curr_graph


class InputStep(Step):
    def __init__(self, name: str, **params) -> None:
        self.name = name
        self.input_validator = params["input_validator"]
        self.out_dir = params["out_dir"]

    def get_subgraph(self, config: "Config") -> MultiDiGraph:
        sub_graph = MultiDiGraph()
        sub_graph.add_node(
            "input_data", input_validator=self.input_validator, out_dir=self.out_dir
        )
        return sub_graph


class ResultStep(Step):
    def __init__(self, name: str, **params) -> None:
        self.name = name
        self.input_validator = params["input_validator"]
        self.out_dir = params["out_dir"]

    def get_subgraph(self, config: "Config") -> MultiDiGraph:
        sub_graph = MultiDiGraph()
        sub_graph.add_node(
            "results", input_validator=self.input_validator, out_dir=self.out_dir
        )
        return sub_graph


class ImplementedStep(Step):
    """Steps contain information about the purpose of the interoperable elements of
    the sequence called a *Pipeline* and how those elements relate to one another.
    In turn, steps are implemented by Implementations, such that each step may have
    several implementations but each implementation must have exactly one step.
    In a sense, steps contain metadata about the implementations to which they relate.
    """

    def __init__(self, name: str, **params) -> None:
        self.name = name
        self.input_validator = params["input_validator"]
        self.out_dir = params["out_dir"]

    def get_subgraph(self, config: "Config") -> MultiDiGraph:
        sub = MultiDiGraph()
        implementation_name = config.get_implementation_name(self.name)
        implementation_config = config.pipeline[self.name]["implementation"]["configuration"]
        implementation = Implementation(
            name=implementation_name,
            step_name=self.name,
            environment_variables=implementation_config.to_dict(),
        )
        sub.add_node(
            implementation_name,
            implementation=implementation,
            input_validator=self.input_validator,
            out_dir=self.out_dir / implementation_name,
        )
        return sub
