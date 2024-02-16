from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from linker.utilities.data_utils import validate_dummy_file


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

    def validate_output(self, step_id: str, results_dir: Path) -> None:
        results_files = list(results_dir.glob("result.parquet"))
        if results_files:
            for results_file in results_files:
                self.validate_file(results_file)
        else:
            raise RuntimeError(
                f"No results found for pipeline step ID {step_id} in results "
                f"directory '{results_dir}'"
            )
