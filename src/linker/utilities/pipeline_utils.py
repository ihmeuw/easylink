import os
import yaml
from pathlib import Path

def get_steps(pipeline_specification: Path) -> Path:
    with open(pipeline_specification, "r") as f:
        pipeline = yaml.full_load(f)
    # TODO: make pipeline implementation generic
    implementation = pipeline["implementation"]
    # TODO: get a handle on exception handling and logging
    if implementation == "pvs_like_python":
        # TODO: stop hard-coding filepaths
        step_dir = (
            Path(os.path.realpath(__file__)).parent.parent.parent.parent
            / "steps"
            / "pvs_like_case_study_sample_data"
        )
    else:
        raise NotImplementedError(f"No support for impementation '{implementation}'")
    return step_dir
