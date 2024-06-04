from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable

from networkx import MultiDiGraph

from easylink.implementation import Implementation
from easylink.utilities.validation_utils import validate_input_file_dummy

if TYPE_CHECKING:
    from easylink.configuration import Config


@dataclass
class Step:
    """Steps contain information about the purpose of the interoperable elements of
    the sequence called a *Pipeline* and how those elements relate to one another.
    In turn, steps are implemented by Implementations, such that each step may have
    several implementations but each implementation must have exactly one step.
    In a sense, steps contain metadata about the implementations to which they relate.
    """

    name: str
    prev_input: bool  # Toy version of TODO [MIC-4774]
    input_files: list  # Toy version of TODO [MIC-4774]
    input_validator: Callable = validate_input_file_dummy

    def get_subgraph(self, config: "Config") -> MultiDiGraph:
        sub = MultiDiGraph()
        sub.add_node(
            config.schema.get_step_id(self),
            implementation=Implementation(config=config, step=self),
            input_validator=self.input_validator,
        )
        return sub
