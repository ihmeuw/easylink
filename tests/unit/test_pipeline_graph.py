from pathlib import Path

import pytest

from easylink.configuration import Config
from easylink.graph_components import InputSlot
from easylink.pipeline_graph import PipelineGraph
from easylink.step import COMBINED_IMPLEMENTATION_KEY
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
            "output_slot_name": "all",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": (
                Path(f"{test_dir}/input_data1/file1.csv"),
                Path(f"{test_dir}/input_data2/file2.csv"),
            ),
        },
        ("input_data", "step_4_python_pandas"): {
            "input_slot_name": "step_4_secondary_input",
            "output_slot_name": "all",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
            "filepaths": (
                Path(f"{test_dir}/input_data1/file1.csv"),
                Path(f"{test_dir}/input_data2/file2.csv"),
            ),
        },
        ("step_1_python_pandas", "step_2_python_pandas"): {
            "input_slot_name": "step_2_main_input",
            "output_slot_name": "step_1_main_output",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": (Path("intermediate/step_1_python_pandas/result.parquet"),),
        },
        ("step_2_python_pandas", "step_3_python_pandas"): {
            "input_slot_name": "step_3_main_input",
            "output_slot_name": "step_2_main_output",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": (Path("intermediate/step_2_python_pandas/result.parquet"),),
        },
        ("step_3_python_pandas", "step_4_python_pandas"): {
            "input_slot_name": "step_4_main_input",
            "output_slot_name": "step_3_main_output",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": (Path("intermediate/step_3_python_pandas/result.parquet"),),
        },
        ("step_4_python_pandas", "results"): {
            "input_slot_name": "result",
            "output_slot_name": "step_4_main_output",
            "validator": validate_input_file_dummy,
            "env_var": None,
            "filepaths": (Path("intermediate/step_4_python_pandas/result.parquet"),),
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
        assert (
            edge_attrs["output_slot"].name
            == expected_edges[(source, sink)]["output_slot_name"]
        )
        assert edge_attrs["filepaths"] == tuple(
            [str(file) for file in expected_edges[(source, sink)]["filepaths"]]
        )


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


def test_update_slot_filepaths(default_config: Config, test_dir: str) -> None:
    pipeline_graph = PipelineGraph(default_config)
    pipeline_graph.update_slot_filepaths(default_config)
    expected_filepaths = {
        ("input_data", "step_1_python_pandas"): (
            str(Path(f"{test_dir}/input_data1/file1.csv")),
            str(Path(f"{test_dir}/input_data2/file2.csv")),
        ),
        ("input_data", "step_4_python_pandas"): (
            str(Path(f"{test_dir}/input_data1/file1.csv")),
            str(Path(f"{test_dir}/input_data2/file2.csv")),
        ),
        ("step_1_python_pandas", "step_2_python_pandas"): (
            str(Path("intermediate/step_1_python_pandas/result.parquet")),
        ),
        ("step_2_python_pandas", "step_3_python_pandas"): (
            str(Path("intermediate/step_2_python_pandas/result.parquet")),
        ),
        ("step_3_python_pandas", "step_4_python_pandas"): (
            str(Path("intermediate/step_3_python_pandas/result.parquet")),
        ),
        ("step_4_python_pandas", "results"): (
            str(Path("intermediate/step_4_python_pandas/result.parquet")),
        ),
    }
    for source, sink, edge_attrs in pipeline_graph.edges(data=True):
        assert edge_attrs["filepaths"] == expected_filepaths[(source, sink)]


def test_get_input_slots(default_config: Config, test_dir: str) -> None:
    expected = {
        "step_1_python_pandas": {
            "step_1_main_input": {
                "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                "validator": validate_input_file_dummy,
                "files": [
                    Path(f"{test_dir}/input_data1/file1.csv"),
                    Path(f"{test_dir}/input_data2/file2.csv"),
                ],
            }
        },
        "step_2_python_pandas": {
            "step_2_main_input": {
                "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                "validator": validate_input_file_dummy,
                "files": [Path("intermediate/step_1_python_pandas/result.parquet")],
            }
        },
        "step_3_python_pandas": {
            "step_3_main_input": {
                "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                "validator": validate_input_file_dummy,
                "files": [Path("intermediate/step_2_python_pandas/result.parquet")],
            }
        },
        "step_4_python_pandas": {
            "step_4_main_input": {
                "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                "validator": validate_input_file_dummy,
                "files": [Path("intermediate/step_3_python_pandas/result.parquet")],
            }
        },
    }
    pipeline_graph = PipelineGraph(default_config)
    for node, expected_slots in expected.items():
        slots = pipeline_graph.get_input_slots(node)
        for slot_name, expected_slot in expected_slots.items():
            slot = slots[slot_name]
            assert slot["env_var"] == expected_slot["env_var"]
            assert slot["validator"] == expected_slot["validator"]
            assert slot["filepaths"] == [str(file) for file in expected_slot["files"]]


def test_condense_input_slots() -> None:
    input_slots = [
        InputSlot(
            "step_1_main_input",
            "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            validate_input_file_dummy,
        ),
        InputSlot(
            "step_1_main_input",
            "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            validate_input_file_dummy,
        ),
        InputSlot(
            "step_2_main_input",
            "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            validate_input_file_dummy,
        ),
        InputSlot(
            "step_2_main_input",
            "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            validate_input_file_dummy,
        ),
    ]
    filepaths_by_slot = [
        ["file1", "file2"],
        ["file3", "file4"],
        ["file5", "file6"],
        ["file7", "file8"],
    ]
    expected = {
        "step_1_main_input": {
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "validator": validate_input_file_dummy,
            "filepaths": ["file1", "file2", "file3", "file4"],
        },
        "step_2_main_input": {
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "validator": validate_input_file_dummy,
            "filepaths": ["file5", "file6", "file7", "file8"],
        },
    }
    condensed_slots = PipelineGraph.condense_input_slots(input_slots, filepaths_by_slot)
    for slot_name, expected_slot in expected.items():
        slot = condensed_slots[slot_name]
        assert slot["env_var"] == expected_slot["env_var"]
        assert slot["validator"] == expected_slot["validator"]
        assert slot["filepaths"] == expected_slot["filepaths"]


def test_condense_input_slots_duplicate_slots() -> None:
    input_slots = [
        InputSlot(
            "step_1_main_input",
            "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            validate_input_file_dummy,
        ),
        InputSlot(
            "step_1_main_input",
            "DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
            validate_input_file_dummy,
        ),
    ]
    filepaths_by_slot = [
        ["file1", "file2"],
        ["file3", "file4"],
    ]
    with pytest.raises(ValueError):
        PipelineGraph.condense_input_slots(input_slots, filepaths_by_slot)


def test_condense_input_slots_duplicate_slots() -> None:
    input_slots = [
        InputSlot(
            "step_1_main_input",
            "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            validate_input_file_dummy,
        ),
        InputSlot(
            "step_1_main_input", "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS", lambda x: None
        ),
    ]
    filepaths_by_slot = [
        ["file1", "file2"],
        ["file3", "file4"],
    ]
    with pytest.raises(ValueError):
        PipelineGraph.condense_input_slots(input_slots, filepaths_by_slot)


def test_get_input_output_files(default_config: Config, test_dir: str) -> None:
    expected = {
        "step_1_python_pandas": (
            [
                f"{test_dir}/input_data1/file1.csv",
                f"{test_dir}/input_data2/file2.csv",
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
                f"{test_dir}/input_data2/file2.csv",
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
        config_params["pipeline"]["steps"]["step_1"]["implementation"][
            "name"
        ] = "step_1_python_pyspark_distributed"
    config = Config(config_params)
    pipeline_graph = PipelineGraph(config)
    assert pipeline_graph.spark_is_required() == requires_spark


def test_merge_joint_implementations(default_config_params, test_dir) -> None:
    config_params = default_config_params
    # make step 3 and step 4 a combined implementations
    config_params["pipeline"]["steps"]["step_3"][COMBINED_IMPLEMENTATION_KEY] = "step_3_4"
    config_params["pipeline"]["steps"]["step_4"][COMBINED_IMPLEMENTATION_KEY] = "step_3_4"
    config_params["pipeline"]["combined_implementations"] = {
        "step_3_4": {
            "name": "step_3_and_step_4_joint_python_pandas",
        }
    }
    pipeline_graph = PipelineGraph(Config(config_params))
    assert set(pipeline_graph.nodes) == {
        "input_data",
        "step_1_python_pandas",
        "step_2_python_pandas",
        "step_3_4",
        "results",
    }
    expected_edges = {
        ("input_data", "step_1_python_pandas"): {
            "input_slot_name": "step_1_main_input",
            "output_slot_name": "all",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": (
                Path(f"{test_dir}/input_data1/file1.csv"),
                Path(f"{test_dir}/input_data2/file2.csv"),
            ),
        },
        ("input_data", "step_3_4"): {
            "input_slot_name": "step_4_secondary_input",
            "output_slot_name": "all",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
            "filepaths": (
                Path(f"{test_dir}/input_data1/file1.csv"),
                Path(f"{test_dir}/input_data2/file2.csv"),
            ),
        },
        ("step_1_python_pandas", "step_2_python_pandas"): {
            "input_slot_name": "step_2_main_input",
            "output_slot_name": "step_1_main_output",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": (Path("intermediate/step_1_python_pandas/result.parquet"),),
        },
        ("step_2_python_pandas", "step_3_4"): {
            "input_slot_name": "step_3_main_input",
            "output_slot_name": "step_2_main_output",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": (Path("intermediate/step_2_python_pandas/result.parquet"),),
        },
        ("step_3_4", "results"): {
            "input_slot_name": "result",
            "output_slot_name": "step_4_main_output",
            "validator": validate_input_file_dummy,
            "env_var": None,
            "filepaths": (Path("intermediate/step_3_4/result.parquet"),),
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
        assert (
            edge_attrs["output_slot"].name
            == expected_edges[(source, sink)]["output_slot_name"]
        )
        assert edge_attrs["filepaths"] == tuple(
            [str(file) for file in expected_edges[(source, sink)]["filepaths"]]
        )
