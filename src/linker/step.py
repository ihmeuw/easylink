from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List


class StepInput:
    """StepInput contains information about the input data for a step in a pipeline. It is used to bind input data to the container environment."""

    def __init__(
        self,
        env_var: str,
        container_dir_name: str,
        input_filenames: List[str],
        prev_output: bool,
    ):
        self.env_var = env_var
        self.container_dir_name = container_dir_name
        self.input_filenames = input_filenames
        self.prev_output = prev_output
        self.host_filepaths = []

    @property
    def container_paths(self) -> List[str]:
        return [
            str(Path(self.container_dir_name) / Path(path).name)
            for path in self.host_filepaths
        ]

    @property
    def bindings(self) -> Dict[str, str]:
        return {
            path: container_path
            for path, container_path in zip(self.host_filepaths, self.container_paths)
        }

    def add_bindings(self, paths: List[Path]) -> None:
        self.host_filepaths.extend(paths)

    def validate_input_filenames(self, input_data: Dict[str, Any]) -> Dict[str, str]:
        errors = {}
        for filename in self.input_filenames:
            if filename not in input_data:
                errors[
                    filename
                ] = f"Step requires input data key {filename} but it was not found in the input data."
        return errors

    def add_input_filename_bindings(self, input_data: Dict[str, Any]) -> None:
        for filename in self.input_filenames:
            self.host_filepaths.append(str(input_data[filename]))


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

    def validate_input_filenames(self, input_data: Dict[str, Any]) -> Dict[str, str]:
        errors = {}
        for input in self.inputs:
            input_errors = input.validate_input_filenames(input_data)
            if input_errors:
                errors[input.env_var] = input_errors
        return errors

    def add_input_filename_bindings(self, input_data: Dict[str, Any]) -> None:
        for input in self.inputs:
            input.add_input_filename_bindings(input_data)
