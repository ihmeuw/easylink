from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Callable

from networkx import MultiDiGraph

from easylink.implementation import Implementation

if TYPE_CHECKING:
    from easylink.configuration import Config


class AbstractStep(ABC):
    def __init__(self, name: str, input_validator: Callable, out_dir: Path) -> None:
        self.name = name
        self.input_validator = input_validator
        self.out_dir = out_dir

    @abstractmethod
    def get_subgraph(self, config: "Config") -> MultiDiGraph:
        pass

class GraphStep(AbstractStep):
    
    def __init__(self, name: str, input_validator: Callable, out_dir: Path, graph_params:dict) -> None:
        self.name = name
        self.input_validator = input_validator
        self.graph = self._create_graph(graph_params)

    def _create_graph(self, graph_params: dict) -> MultiDiGraph:
        graph = MultiDiGraph()
        for step_name, step_params in graph_params.items():
            graph.add_node(
                step_name,
                step=step_params["step_type"](
                    step_name,
                    input_validator=step_params["input_validator"],
                    out_dir=step_params["out_dir"],
                ),
            )
            for in_edge, edge_params in step_params["in_edges"].items():
                graph.add_edge(in_edge, step_name, **edge_params)

        return graph

    def get_subgraph(self, config: Config) -> MultiDiGraph:
        sub_graph = MultiDiGraph()
        sub_graph.update(self.graph)
        for subgraph_node in self.graph.nodes:
            node_graph = self.graph.nodes[subgraph_node]["step"].get_subgraph(config)
            sub_graph.update(node_graph)
            sub_source_nodes = [
                node for node in node_graph.nodes if node_graph.in_degree(node) == 0
            ]
            input_edges = [
                (source, subgraph_node, data)
                for source, _, data in sub_graph.in_edges(subgraph_node, data=True)
            ]
            for source, _, data in input_edges:
                sub_graph.add_edges_from([(source, node, data) for node in sub_source_nodes])

            output_edges = [
                (subgraph_node, sink, data)
                for _, sink, data in sub_graph.out_edges(subgraph_node, data=True)
            ]
            sub_sink_nodes = [
                node for node in sub_graph.nodes if sub_graph.out_degree(node) == 0
            ]
            # Get absolute paths from relative paths
            for _, sink, data in output_edges:
                for sub_node in sub_sink_nodes:
                    data["files"] = [
                        str(sub_graph.nodes[sub_node]["out_dir"] / file)
                        for file in data["files"]
                    ]
                    sub_graph.add_edge(sub_node, sink, data=data)
            # Remove the original node
            sub_graph.remove_node(subgraph_node)
        return sub_graph
        


class InputStep(AbstractStep):
    def get_subgraph(self, config: "Config") -> MultiDiGraph:
        sub_graph = MultiDiGraph()
        sub_graph.add_node(
            "input_data", input_validator=self.input_validator, out_dir=self.out_dir
        )
        return sub_graph


class ResultStep(AbstractStep):
    def get_subgraph(self, config: "Config") -> MultiDiGraph:
        sub_graph = MultiDiGraph()
        sub_graph.add_node(
            "results", input_validator=self.input_validator, out_dir=self.out_dir
        )
        return sub_graph


class ImplementedStep(AbstractStep):
    """Steps contain information about the purpose of the interoperable elements of
    the sequence called a *Pipeline* and how those elements relate to one another.
    In turn, steps are implemented by Implementations, such that each step may have
    several implementations but each implementation must have exactly one step.
    In a sense, steps contain metadata about the implementations to which they relate.
    """

    def __init__(self, name: str, input_validator: Callable, out_dir: Path) -> None:
        self.name = name
        self.input_validator = input_validator
        self.out_dir = out_dir

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
