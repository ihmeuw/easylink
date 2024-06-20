from pathlib import Path

import networkx as nx
import pytest

from easylink.configuration import Config
from easylink.pipeline_graph import PipelineGraph
from easylink.pipeline_schema_constants import SCHEMA_EDGES, SCHEMA_NODES
from easylink.utilities.data_utils import load_yaml
from easylink.utilities.validation_utils import validate_input_file_dummy


def test__create_graph(default_config: Config, test_dir: str) -> None:
    pipeline_graph = PipelineGraph(default_config)
    assert set(pipeline_graph.nodes) == {
        "input_data",
        "step_1_python_pandas",
        "step_2_python_pandas",
        "step_3_python_pandas",
        "step_4_python_pandas",
        "results",
    }
    expected_edges = {
        ("input_data", "step_1_python_pandas"): {
            "input_slot_name": "step_1_main_input",
            "output_slot_name": "file1",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": [Path(f"{test_dir}/input_data1/file1.csv")],
        },
        ("input_data", "step_4_python_pandas"): {
            "input_slot_name": "step_4_secondary_input",
            "output_slot_name": "file1",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
            "filepaths": [Path(f"{test_dir}/input_data1/file1.csv")],
        },
        ("step_1_python_pandas", "step_2_python_pandas"): {
            "input_slot_name": "step_2_main_input",
            "output_slot_name": "step_1_main_output",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": [Path("intermediate/step_1_python_pandas/result.parquet")],
        },
        ("step_2_python_pandas", "step_3_python_pandas"): {
            "input_slot_name": "step_3_main_input",
            "output_slot_name": "step_2_main_output",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": [Path("intermediate/step_2_python_pandas/result.parquet")],
        },
        ("step_3_python_pandas", "step_4_python_pandas"): {
            "input_slot_name": "step_4_main_input",
            "output_slot_name": "step_3_main_output",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": [Path("intermediate/step_3_python_pandas/result.parquet")],
        },
        ("step_4_python_pandas", "results"): {
            "input_slot_name": "result",
            "output_slot_name": "step_4_main_output",
            "validator": validate_input_file_dummy,
            "env_var": None,
            "filepaths": [Path("intermediate/step_4_python_pandas/result.parquet")],
        },
    }
    assert set(pipeline_graph.edges()) == expected_edges.keys()
    for source, sink, edge_attrs in pipeline_graph.edges(data=True):
        assert (
            edge_attrs["input_slot"].name == expected_edges[(source, sink)]["input_slot_name"]
        )
        assert edge_attrs["input_slot"].env_var == expected_edges[(source, sink)]["env_var"]
        assert (
            edge_attrs["input_slot"].validator == expected_edges[(source, sink)]["validator"]
        )
        assert edge_attrs["output_slot"] == expected_edges[(source, sink)]["output_slot_name"]
        assert edge_attrs["filepaths"] == [
            str(file) for file in expected_edges[(source, sink)]["filepaths"]
        ]


def test_implementations(default_config: Config) -> None:
    pipeline_graph = PipelineGraph(default_config)
    implementation_names = [
        implementation.name for implementation in pipeline_graph.implementations
    ]
    expected_names = [
        "step_1_python_pandas",
        "step_2_python_pandas",
        "step_3_python_pandas",
        "step_4_python_pandas",
    ]
    assert implementation_names == expected_names
    assert pipeline_graph.implementation_nodes == expected_names


def test_get_input_output_files(default_config: Config, test_dir: str) -> None:
    expected = {
        "step_1_python_pandas": (
            [
                f"{test_dir}/input_data1/file1.csv",
            ],
            ["intermediate/step_1_python_pandas/result.parquet"],
        ),
        "step_2_python_pandas": (
            ["intermediate/step_1_python_pandas/result.parquet"],
            ["intermediate/step_2_python_pandas/result.parquet"],
        ),
        "step_3_python_pandas": (
            ["intermediate/step_2_python_pandas/result.parquet"],
            ["intermediate/step_3_python_pandas/result.parquet"],
        ),
        "step_4_python_pandas": (
            [
                f"{test_dir}/input_data1/file1.csv",
                "intermediate/step_3_python_pandas/result.parquet",
            ],
            ["intermediate/step_4_python_pandas/result.parquet"],
        ),
    }
    pipeline_graph = PipelineGraph(default_config)
    for node, (expected_input_files, expected_output_files) in expected.items():
        input_files, output_files = pipeline_graph.get_input_output_files(node)
        assert input_files == expected_input_files
        assert output_files == expected_output_files


@pytest.mark.parametrize("requires_spark", [True, False])
def test_spark_is_required(default_config_params, requires_spark):
    config_params = default_config_params
    if requires_spark:
        # Change step 1's implementation to python_pyspark
        config_params["pipeline"]["step_1"]["implementation"][
            "name"
        ] = "step_1_python_pyspark_distributed"
    config = Config(config_params)
    pipeline_graph = PipelineGraph(config)
    assert pipeline_graph.spark_is_required() == requires_spark
