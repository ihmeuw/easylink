from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path


class Rule(ABC):
    def write_to_snakefile(self, snakefile_dir):
        snakefile = snakefile_dir / "Snakefile"
        with open(snakefile, "a") as f:
            f.write(self._build_rule())

    @abstractmethod
    def _build_rule(self) -> str:
        pass


@dataclass
class ImplementedRule(Rule):
    name: str
    input: list[str]
    output: list[str]
    script_path: str

    def _build_rule(self) -> str:
        return f"""
rule:
    name: "{self.name}"
    input: {self.input}           
    output: {self.output}
    script: "{self.script_path}"
                """


@dataclass
class ValidationRule(Rule):
    name: str
    input: list[str]
    output: str
    validator: str

    def _build_rule(self):
        return f"""
rule:
    name: "{self.name}_validator"
    input: {self.input}
    output: touch("{self.output}")
    priority: 1
    localrule: True           
    run:
        print("Validating {self.name} output")
        for f in input:
            {self.validator}(f)
        touch(output)
                """
