from pathlib import Path
from typing import TYPE_CHECKING, Callable, Tuple

from linker.configuration import Config
from linker.implementation import Implementation
from linker.step import Step

if TYPE_CHECKING:
    from linker.pipeline_schema import PipelineSchema


class Pipeline:
    """Abstraction to handle pipeline specification and execution."""

    def __init__(self, config: Config):
        self.config = config
        self.steps = self._get_steps()
        self.implementations = self._get_implementations()

    def run(self, runner: Callable, results_dir: Path) -> None:
        for implementation in self.implementations:
            implementation.run(
                runner=runner,
                container_engine=self.config.container_engine,
                input_data=self.config.input_data,
                results_dir=results_dir,
            )

    def _validate(self, schema: "PipelineSchema") -> None:
        for idx, implementation in enumerate(self.implementations):
            if implementation.step_name != schema.steps[idx].name:
                return False
        return True

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
