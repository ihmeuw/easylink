from dataclasses import dataclass
from pathlib import Path

@dataclass
class Rule:
    name: str
    input: list[str]
    output: str
    script: str
    snakefile_dir: Path
        
    def write_to_snakefile(self):
        snakefile = self.snakefile_dir / "Snakefile"
        with open(snakefile, "a") as f:
            f.write(f"""
rule:
    name: "{self.name}"
    input: {self.input}           
    output: "{self.output}"
    script: "{self.script}"
                """)