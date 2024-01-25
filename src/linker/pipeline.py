from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

from linker.configuration import Config
from linker.implementation import Implementation
from linker.utilities.general_utils import exit_with_validation_error


class Pipeline:
    """Abstraction to handle pipeline specification and execution."""

    def __init__(self, config: Config):
        self.config = config
        self.implementations = self._get_implementations()
        self._validate()

    def get_implementation_name(self, step_name: str) -> str:
        return self.config.pipeline["steps"][step_name]["implementation"]["name"]

    def get_implementation_config(self, step_name: str) -> Optional[Dict[str, Any]]:
        return self.config.pipeline["steps"][step_name]["implementation"].get(
            "configuration", None
        )

    def _get_implementations(self) -> Tuple[Implementation, ...]:
        resources = {key: self.config.environment.get(key) for key in ["slurm", "spark"]}
        return tuple(
            Implementation(
                step=step,
                implementation_name=self.get_implementation_name(step.name),
                implementation_config=self.get_implementation_config(step.name),
                container_engine=self.config.container_engine,
                resources=resources,
            )
            for step in self.config.schema.steps
        )

    def _validate(self) -> None:
        """Validates the pipeline."""

        errors = {**self._validate_implementations()}

        if errors:
            exit_with_validation_error(errors)

    def _validate_implementations(self) -> Dict:
        """Validates each individual Implementation instance."""
        errors = defaultdict(dict)
        for implementation in self.implementations:
            implementation_errors = implementation.validate()
            if implementation_errors:
                errors["IMPLEMENTATION ERRORS"][implementation.name] = implementation_errors
        return errors

    def run(
        self,
        runner: Callable,
        results_dir: Path,
        session: Optional["drmaa.Session"],
    ) -> None:
        number_of_steps = len(self.implementations)
        number_of_steps_digit_length = len(str(number_of_steps))
        for idx, implementation in enumerate(self.implementations):
            step_number = str(idx + 1).zfill(number_of_steps_digit_length)
            previous_step_number = str(idx).zfill(number_of_steps_digit_length)
            step_id = f"{step_number}_{implementation.step_name}"
            diagnostics_dir = results_dir / "diagnostics" / step_id
            diagnostics_dir.mkdir(parents=True, exist_ok=True)
            output_dir = (
                results_dir
                if idx == (number_of_steps - 1)
                else results_dir
                / "intermediate"
                / f"{step_number}_{implementation.step_name}"
            )
            input_data = self.config.input_data
            if idx <= number_of_steps - 1:
                output_dir.mkdir(exist_ok=True)
            if idx > 0:
                # Overwrite the pipeline input data with the results of the previous step
                input_data = [
                    file
                    for file in (
                        output_dir
                        / "intermediate"
                        / f"{previous_step_number}_{self.implementations[idx - 1].step_name}"
                    ).glob("*.parquet")
                ]
            implementation.run(
                session=session,
                runner=runner,
                container_engine=self.config.container_engine,
                step_id=step_id,
                input_data=input_data,
                results_dir=output_dir,
                diagnostics_dir=diagnostics_dir,
            )
        # Close the drmaa session (if one exists) once the pipeline is finished
        if session:
            session.exit()
