import os
from pathlib import Path
from typing import Callable, Tuple

from linker.configuration import Config
from linker.implementation import Implementation
from linker.step import Step


class Pipeline:
    """Abstraction to handle pipeline specification and execution."""

    def __init__(self, config: Config):
        self.config = config
        self.steps = self._get_steps()
        self.implementations = self._get_implementations()
        self.runner = None  # Assigned later from set_runner method

    def set_runner(self, runner: Callable) -> None:
        self.runner = runner

    def run(self, results_dir: Path) -> None:
        if not self.runner:
            raise RuntimeError("Runner has not been set.")
        for implementation in self.implementations:
            implementation.run(
                runner=self.runner,
                container_engine=self.config.container_engine,
                input_data=self.config.input_data,
                results_dir=results_dir,
            )

    #################
    # Setup methods #
    #################

    def _get_steps(self) -> Tuple[Step, ...]:
        return tuple(Step(step) for step in self.config.pipeline["steps"])

    def _get_implementations(self) -> Tuple[Implementation, ...]:
        implementations = []
        for step in self.steps:
            implementation_name = self.config.pipeline["steps"][step.name]["implementation"]
            implementations.append(Implementation(step.name, implementation_name))
        return tuple(implementations)
