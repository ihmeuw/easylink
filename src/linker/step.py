from pathlib import Path
from typing import Callable, List

from linker.implementation import Implementation


class Step:
    IMPLEMENTATIONS = {
        "pvs_like_case_study": [
            "pvs_like_python",
            "pvs_like_r",
            "pvs_like_spark_cluster",
            "pvs_like_spark_local",
        ],
    }

    def __init__(self, name, config):
        self.name = name
        self.config = config
        self.implementation = Implementation(
            self.name, self.config, self.allowable_implementations
        )

    def __repr__(self):
        return self.name

    @property
    def allowable_implementations(self) -> List[str]:
        if self.name not in self.IMPLEMENTATIONS:
            raise ValueError(
                f"Step '{self.name}' is not supported.\n"
                f"Supported steps: {list(self.IMPLEMENTATIONS.keys())}"
            )
        return self.IMPLEMENTATIONS[self.name]

    def run(
        self,
        runner: Callable,
        container_engine: str,
        input_data: List[str],
        results_dir: Path,
    ) -> None:
        self.implementation.run(
            runner=runner,
            container_engine=container_engine,
            input_data=input_data,
            results_dir=results_dir,
        )
