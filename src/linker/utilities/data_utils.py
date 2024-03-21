import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import yaml


def get_results_directory(output_dir: Optional[str], timestamp: bool) -> Path:
    results_dir = Path("results" if output_dir is None else output_dir).resolve()
    if timestamp:
        launch_time = _get_timestamp()
        results_dir = results_dir / launch_time
    return results_dir


def _get_timestamp():
    return datetime.now().strftime("%Y_%m_%d_%H_%M_%S")


def load_yaml(filepath: Path) -> Dict:
    with open(filepath, "r") as file:
        data = yaml.safe_load(file)
    return data
