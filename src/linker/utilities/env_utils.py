from pathlib import Path
from typing import Dict, Union

import yaml


def get_compute_config(computing_environment: str) -> Dict[str, Union[Dict, str]]:
    if computing_environment == "local":
        return {"computing_environment": "local"}
    else:
        compute_env = Path(computing_environment).resolve()
        if not compute_env.is_file():
            raise RuntimeError(
                "Computing environment is expected to be either 'local' or a path "
                f"to an existing yaml file. Input is neither: '{computing_environment}'"
            )
    with open(compute_env, "r") as f:
        compute_env = yaml.full_load(f)

    return compute_env
