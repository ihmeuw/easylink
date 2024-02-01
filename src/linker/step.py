from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List


@dataclass
class StepInput:
    """StepInput contains information about the input data for a step in a pipeline. It is used to bind input data to the container environment."""

    env_var: str
    dir_name: str
    filepaths: List[str]
    prev_output: bool

    @property
    def container_paths(self) -> List[str]:
        return [str(Path(self.dir_name) / Path(path).name) for path in self.filepaths]

    @property
    def bindings(self) -> Dict[str, str]:
        return {
            path: container_path
            for path, container_path in zip(self.filepaths, self.container_paths)
        }

    def add_bindings(self, paths: List[Path]) -> None:
        self.filepaths.extend(paths)


class Step:
    """Steps contain information about the purpose of the interoperable elements of the sequence called a *Pipeline* and how those elements relate to one another.
    In turn, steps are implemented by Implementations. In a sense, steps contain metadata about the implementations to which they relate.
    """

    def __init__(self, name: str, validate_output: Callable, inputs: Dict[str, Any] = {}):
        self.name = name
        self.validate_output = validate_output
        self.inputs = [
            StepInput(env_var, **input_params) for env_var, input_params in inputs.items()
        ]

    def add_bindings_from_prev(self, paths: List[Path]):
        input_from_prev = [input for input in self.inputs if input.prev_output]
        for input in input_from_prev:
            input.add_bindings(paths)
