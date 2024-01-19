from dataclasses import dataclass
from typing import Callable

from linker.utilities.data_utils import validate_dummy_output


@dataclass
class Step:
    """A convenience container in the event we ever want to add step-level functionality"""

    name: str
    validate_output: Callable = validate_dummy_output
