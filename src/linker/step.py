from dataclasses import dataclass
from typing import Callable

from linker.utilities.validation_utils import validate_dummy_file


@dataclass
class Step:
    """Steps contain information about the purpose of the interoperable elements of
    the sequence called a *Pipeline* and how those elements relate to one another.
    In turn, steps are implemented by Implementations, such that each step may have
    several implementations but each implementation must have exactly one step.
    In a sense, steps contain metadata about the implementations to which they relate.
    """

    name: str
    validate_file: Callable = validate_dummy_file
