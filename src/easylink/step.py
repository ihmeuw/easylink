from dataclasses import dataclass
from typing import Callable

from easylink.implementation import Implementation
from easylink.utilities.validation_utils import validate_input_file_dummy


@dataclass
class Step:
    """Steps contain information about the purpose of the interoperable elements of
    the sequence called a *Pipeline* and how those elements relate to one another.
    In turn, steps are implemented by Implementations, such that each step may have
    several implementations but each implementation must have exactly one step.
    In a sense, steps contain metadata about the implementations to which they relate.
    """

    name: str
    input_validator: Callable = validate_input_file_dummy

    def get_subgraph(self, config):
        return Implementation(config=config, step=self)
