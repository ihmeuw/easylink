import os
from pathlib import Path
from typing import Callable, Dict, Tuple

from loguru import logger

from linker.configuration import Config
from linker.step import Step
from linker.utilities.general_utils import load_yaml


class Pipeline:
    """Abstraction to handle pipeline specification and execution."""

    def __init__(self, config: Config):
        self.config = config
        self.steps = self._get_steps()
        self.implementation_metadata = self._load_implementation_metadata()
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
                step_name=step.name,
                implementation_dir=self._get_implementation_directory(step.name),
                container_full_stem=self._get_container_full_stem(step.name),
            )

    #################
    # Setup methods #
    #################

    def _get_steps(self) -> Tuple[Step, ...]:
        spec_steps = tuple(self.config.pipeline["steps"])
        steps = tuple([Step(step) for step in spec_steps if step in Step.IMPLEMENTATIONS])
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

    def _load_implementation_metadata(self) -> Dict[str, Path]:
        implementation_metadata = {}
        for step in [step.name for step in self.steps]:
            implementation_dir = self._get_implementation_directory(step)
            metadata_path = implementation_dir / "metadata.yaml"
            if metadata_path.exists():
                metadata = load_yaml(metadata_path)
                implementation_metadata[step] = metadata
            else:
                implementation_metadata[step] = None
        return implementation_metadata

    ##################
    # Helper methods #
    ##################

    def _get_implementation_directory(self, step_name: str) -> Path:
        # TODO: move this into proper config validator
        implementation = self.config.pipeline["steps"][step_name]["implementation"]
        implementation_dir = (
            Path(os.path.realpath(__file__)).parent / "steps" / step_name / "implementations"
        )
        implementation_names = [
            str(d.name) for d in implementation_dir.iterdir() if d.is_dir()
        ]
        if implementation in implementation_names:
            implementation_dir = implementation_dir / implementation
        else:
            raise NotImplementedError(
                f"No support found for step '{step_name}', implementation '{implementation}'."
            )
        return implementation_dir

    def _get_container_full_stem(self, step_name: str) -> str:
        container_dict = self.implementation_metadata[step_name]["image"]
        return f"{container_dict['directory']}/{container_dict['filename']}"
