from dataclasses import dataclass
from typing import Callable

from linker.utilities.general_utils import dummy_output_validator


@dataclass
class Step:
    """A convenience container in the event we ever want to add step-level functionality"""

    name: str
    output_validator: Callable = dummy_output_validator
