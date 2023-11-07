from typing import List, Callable
from pathlib import Path


class Step:

    IMPLEMENTATIONS = {
        "pvs_like_case_study": [
            "pvs_like_python",
            "pvs_like_r",
            "pvs_like_spark_cluster",
            "pvs_like_spark_local",
        ],
    }

    def __init__(self, name):
        self.name = name

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
    
    # @property
    # def implementation(self, name, config) -> Implementation:
    #     if name not in self.allowable_implementations:
    #         raise ValueError(
    #             f"Implementation '{name}' is not supported for step '{self.name}'.\n"
    #             f"Supported implementations: {self.allowable_implementations}"
    #         )
    #     return Implementation(name, config)

    def run(self,
        runner: Callable,
        container_engine: str,
        input_data: List[str],
        results_dir: Path,
        step_name: str,
        implementation_dir: Path,
        container_full_stem: str,
    ) -> None:
        runner(
            container_engine=container_engine,
            input_data=input_data,
            results_dir=results_dir,
            step_name=step_name,
            implementation_dir=implementation_dir,
            container_full_stem=container_full_stem, 
        )
