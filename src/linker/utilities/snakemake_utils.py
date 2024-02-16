from linker.utilities.data_utils import load_yaml
import tempfile
import yaml
from pathlib import Path


def make_config(pipeline_specification,input_data, computing_environment,results_dir) -> None:
    snake_config = {"pipeline_specification": str(pipeline_specification),
                    "input_data": str(input_data),
                    "environment": str(computing_environment),
                    "results_dir": str(results_dir),
                    "output_file": f"{results_dir}/result.parquet",}
    with tempfile.NamedTemporaryFile("w", delete=False, suffix=".yaml") as snake_config_file:
        yaml.dump(snake_config, snake_config_file)
    return snake_config_file.name
    
    