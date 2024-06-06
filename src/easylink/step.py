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
