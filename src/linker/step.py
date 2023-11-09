from dataclasses import dataclass


@dataclass
class Step:
    """A convenience container in the event we ever want to add step-level functionality"""

    name: str
