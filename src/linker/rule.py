from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable


class Rule(ABC):
    def write_to_snakefile(self, snakefile_dir) -> None:
        snakefile = snakefile_dir / "Snakefile"
        with open(snakefile, "a") as f:
            f.write(self._build_rule())

    @abstractmethod
    def _build_rule(self) -> str:
        pass


@dataclass
class TargetRule(Rule):
    target_files: list[str]
    validation: str

    def _build_rule(self) -> str:
        return f"""
rule all:
    input:
        final_output={self.target_files},
        validator="{self.validation}"
                """


@dataclass
class ImplementedRule(Rule):
    name: str
    execution_input: list[str]
    validation: str
    output: list[str]
    script_path: str

    def _build_rule(self) -> str:
        return f"""
rule:
    name: "{self.name}"
    input: 
        implementation_inputs={self.execution_input},
        validation="{self.validation}"           
    output: {self.output}
    script: "{self.script_path}"
                """


@dataclass
class ValidationRule(Rule):
    name: str
    input: list[str]
    output: str
    validator: Callable

    def _build_rule(self) -> str:
        return f"""
rule:
    name: "{self.name}_validator"
    input: {self.input}
    output: temp(touch("{self.output}"))
    localrule: True         
    message: "Validating {self.name} input"
    run:
        for f in input:
            {self.validator.__name__}(f)
                """
