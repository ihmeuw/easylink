from pathlib import Path
from typing import Callable, Tuple

from loguru import logger

from linker.configuration import Config
from linker.step import Step


class Pipeline:
    """Abstraction to handle pipeline specification and execution."""

    def __init__(self, config: Config):
        self.config = config
        self.steps = self._get_steps()
        self.runner = None  # Assigned later from set_runner method

    def set_runner(self, runner: Callable) -> None:
        self.runner = runner

    def run(self, results_dir: Path):
        if not self.runner:
            raise RuntimeError("Runner has not been set.")
        for step in self.steps:
            step.run(
                runner=self.runner,
                container_engine=self.config.container_engine,
                input_data=self.config.input_data,
                results_dir=results_dir,
            )

    #################
    # Setup methods #
    #################

    def _get_steps(self) -> Tuple[Step, ...]:
        spec_steps = tuple(self.config.pipeline["steps"])
        # TODO: The order is defined below but should be part of the
        ## PipelineSchema when that becomes a thing
        steps = tuple(
            Step(step, self.config) for step in Step.IMPLEMENTATIONS if step in spec_steps
        )
        unknown_steps = [step for step in spec_steps if step not in Step.IMPLEMENTATIONS]
        if unknown_steps:
            logger.warning(
                f"Unknown steps are included in the pipeline specification: {unknown_steps}.\n"
                "These steps will be ignored.\n"
                f"Supported steps: {list(Step.IMPLEMENTATIONS.keys())}"
            )
        if not steps:
            raise RuntimeError(
                "No supported steps found in pipeline specification.\n"
                f"Steps found in pipeline specification: {spec_steps}\n"
                f"Supported steps: {list(Step.IMPLEMENTATIONS.keys())}"
            )
        return steps
