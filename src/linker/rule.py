from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, List


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
    target_files: List[str]
    validation: str

    def _build_rule(self) -> str:
        return f"""
rule all:
    input:
        final_output={self.target_files},
        validation='{self.validation}'"""


@dataclass
class ImplementedRule(Rule):
    name: str
    execution_input: List[str]
    validation: str
    output: List[str]
    envvars: dict
    diagnostics_dir: str
    script_cmd: str

    def _build_rule(self) -> str:
        return (
            f"""
rule:
    name: "{self.name}"
    input: 
        implementation_inputs={self.execution_input},
        validation="{self.validation}"           
    output: {self.output}"""
            + self._build_shell_command()
        )

    def _build_shell_command(self) -> str:
        shell_cmd = f"""
    shell:
        '''
        export DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS={",".join(self.execution_input)}
        export DUMMY_CONTAINER_OUTPUT_PATHS={",".join(self.output)}
        export DUMMY_CONTAINER_DIAGNOSTICS_DIRECTORY={self.diagnostics_dir}"""
        for var_name, var_value in self.envvars.items():
            shell_cmd += f"""
        export {var_name}={var_value}"""
        shell_cmd += f"""
        {self.script_cmd}
        '''"""

        return shell_cmd


@dataclass
class ValidationRule(Rule):
    name: str
    input: List[str]
    output: str
    validator: Callable

    def _build_rule(self) -> str:
        return f"""
rule:
    name: "{self.name}_validator"
    input: {self.input}
    output: touch("{self.output}")
    localrule: True         
    message: "Validating {self.name} input"
    run:
        for f in input:
            validation_utils.{self.validator.__name__}(f)"""