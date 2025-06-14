import re
from pathlib import Path

import pytest

from easylink.configuration import Config
from easylink.graph_components import InputSlot, OutputSlot
from easylink.pipeline_graph import PipelineGraph
from easylink.utilities.data_utils import load_yaml
from easylink.utilities.validation_utils import validate_input_file_dummy


def test__create_graph(default_config: Config, test_dir: str) -> None:
    pipeline_graph = PipelineGraph(default_config)
    expected_nodes = {
        "input_data",
        "step_1_python_pandas",
        "step_2_python_pandas",
        "step_3_step_3_main_input_split",
        "step_3_python_pandas",
        "step_3_aggregate",
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
        ("step_2_python_pandas", "step_3_step_3_main_input_split"): {
            "input_slot_name": "step_3_main_input",
            "output_slot_name": "step_2_main_output",
            "validator": None,
            "env_var": None,
            "filepaths": (Path("intermediate/step_2_python_pandas/result.parquet"),),
        },
        ("step_3_step_3_main_input_split", "step_3_python_pandas"): {
            "input_slot_name": "step_3_main_input",
            "output_slot_name": "step_3_step_3_main_input_split_main_output",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": (
                Path("intermediate/step_3_step_3_main_input_split/{chunk}/result.parquet"),
            ),
        },
        ("step_3_python_pandas", "step_3_aggregate"): {
            "input_slot_name": "step_3_aggregate_main_input",
            "output_slot_name": "step_3_main_output",
            "validator": None,
            "env_var": None,
            "filepaths": (Path("intermediate/step_3_python_pandas/{chunk}/result.parquet"),),
        },
        ("step_3_aggregate", "step_4_python_pandas"): {
            "input_slot_name": "step_4_main_input",
            "output_slot_name": "step_3_main_output",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": (Path("intermediate/step_3_aggregate/result.parquet"),),
        },
        ("step_4_python_pandas", "results"): {
            "input_slot_name": "result",
            "output_slot_name": "step_4_main_output",
            "validator": validate_input_file_dummy,
            "env_var": None,
            "filepaths": (Path("intermediate/step_4_python_pandas/result.parquet"),),
        },
    }
    _check_nodes_and_edges(pipeline_graph, expected_nodes, expected_edges)


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


@pytest.mark.parametrize(
    "slot_type, is_duplicated, slot_tuples",
    [
        (
            "input_slot",
            False,
            {
                (
                    "step_a",
                    InputSlot("foo", env_var="bar", validator=validate_input_file_dummy),
                ),
                (
                    "step_b",
                    InputSlot("baz", env_var="spam", validator=validate_input_file_dummy),
                ),
            },
        ),
        (
            "input_slot",
            True,
            {
                (
                    "step_a",
                    InputSlot("foo", env_var="bar", validator=validate_input_file_dummy),
                ),
                (
                    "step_b",
                    InputSlot("foo", env_var="spam", validator=validate_input_file_dummy),
                ),
            },
        ),
        (
            "input_slot",
            True,
            {
                (
                    "step_a",
                    InputSlot("foo", env_var="bar", validator=validate_input_file_dummy),
                ),
                (
                    "step_b",
                    InputSlot("baz", env_var="bar", validator=validate_input_file_dummy),
                ),
            },
        ),
        (
            "output_slot",
            False,
            {
                ("step_a", OutputSlot("foo")),
                ("step_b", OutputSlot("baz")),
            },
        ),
        (
            "output_slot",
            True,
            {
                ("step_a", OutputSlot("foo")),
                ("step_b", OutputSlot("foo")),
            },
        ),
    ],
)
def test__get_duplicate_slots(slot_tuples, slot_type, is_duplicated) -> None:
    duplicates = PipelineGraph._get_duplicate_slots(slot_tuples, slot_type)
    if is_duplicated:
        assert duplicates == slot_tuples
    else:
        assert not duplicates


def test_update_slot_filepaths(default_config: Config, test_dir: str) -> None:
    pipeline_graph = PipelineGraph(default_config)
    pipeline_graph._update_slot_filepaths(default_config)
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
            "intermediate/step_1_python_pandas/result.parquet",
        ),
        ("step_2_python_pandas", "step_3_step_3_main_input_split"): (
            "intermediate/step_2_python_pandas/result.parquet",
        ),
        ("step_3_step_3_main_input_split", "step_3_python_pandas"): (
            "intermediate/step_3_step_3_main_input_split/{chunk}/result.parquet",
        ),
        ("step_3_python_pandas", "step_3_aggregate"): (
            "intermediate/step_3_python_pandas/{chunk}/result.parquet",
        ),
        ("step_3_aggregate", "step_4_python_pandas"): (
            "intermediate/step_3_aggregate/result.parquet",
        ),
        ("step_4_python_pandas", "results"): (
            "intermediate/step_4_python_pandas/result.parquet",
        ),
    }
    for source, sink, edge_attrs in pipeline_graph.edges(data=True):
        assert edge_attrs["filepaths"] == expected_filepaths[(source, sink)]


def test_get_io_slots(default_config: Config, test_dir: str) -> None:
    expected = {
        "input_data": {
            "input": {},
            "output": {
                "all": {
                    "filepaths": [
                        str(Path(f"{test_dir}/input_data1/file1.csv")),
                        str(Path(f"{test_dir}/input_data2/file2.csv")),
                    ],
                }
            },
        },
        "step_1_python_pandas": {
            "input": {
                "step_1_main_input": {
                    "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    "validator": validate_input_file_dummy,
                    "files": [
                        Path(f"{test_dir}/input_data1/file1.csv"),
                        Path(f"{test_dir}/input_data2/file2.csv"),
                    ],
                },
            },
            "output": {
                "step_1_main_output": {
                    "filepaths": [Path("intermediate/step_1_python_pandas/result.parquet")],
                },
            },
        },
        "step_2_python_pandas": {
            "input": {
                "step_2_main_input": {
                    "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    "validator": validate_input_file_dummy,
                    "files": [Path("intermediate/step_1_python_pandas/result.parquet")],
                },
            },
            "output": {
                "step_2_main_output": {
                    "filepaths": [Path("intermediate/step_2_python_pandas/result.parquet")],
                },
            },
        },
        "step_3_step_3_main_input_split": {
            "input": {
                "step_3_main_input": {
                    "env_var": None,
                    "validator": None,
                    "files": [Path("intermediate/step_2_python_pandas/result.parquet")],
                },
            },
            "output": {
                "step_3_step_3_main_input_split_main_output": {
                    "filepaths": [
                        Path(
                            "intermediate/step_3_step_3_main_input_split/{chunk}/result.parquet"
                        )
                    ],
                },
            },
        },
        "step_3_python_pandas": {
            "input": {
                "step_3_main_input": {
                    "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    "validator": validate_input_file_dummy,
                    "files": [
                        Path(
                            "intermediate/step_3_step_3_main_input_split/{chunk}/result.parquet"
                        )
                    ],
                },
            },
            "output": {
                "step_3_main_output": {
                    "filepaths": [
                        Path("intermediate/step_3_python_pandas/{chunk}/result.parquet")
                    ],
                },
            },
        },
        "step_3_aggregate": {
            "input": {
                "step_3_aggregate_main_input": {
                    "env_var": None,
                    "validator": None,
                    "files": [
                        Path("intermediate/step_3_python_pandas/{chunk}/result.parquet")
                    ],
                },
            },
            "output": {
                "step_3_main_output": {
                    "filepaths": [Path("intermediate/step_3_aggregate/result.parquet")],
                },
            },
        },
        "step_4_python_pandas": {
            "input": {
                "step_4_main_input": {
                    "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    "validator": validate_input_file_dummy,
                    "files": [Path("intermediate/step_3_aggregate/result.parquet")],
                },
                "step_4_secondary_input": {
                    "env_var": "DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
                    "validator": validate_input_file_dummy,
                    "files": [
                        Path(f"{test_dir}/input_data1/file1.csv"),
                        Path(f"{test_dir}/input_data2/file2.csv"),
                    ],
                },
            },
            "output": {
                "step_4_main_output": {
                    "filepaths": [Path("intermediate/step_4_python_pandas/result.parquet")],
                },
            },
        },
        "results": {
            "input": {
                "result": {
                    "env_var": None,
                    "validator": validate_input_file_dummy,
                    "files": [Path("intermediate/step_4_python_pandas/result.parquet")],
                }
            },
            "output": {},
        },
    }
    pipeline_graph = PipelineGraph(default_config)
    # for node, expected_slots in expected.items():
    for node in pipeline_graph.nodes:
        # expected_slots = expected[node]
        input_slots, output_slots = pipeline_graph.get_io_slot_attributes(node)
        # for slot_name, expected_slot in expected_slots["input"].items():
        for slot_name, slot in input_slots.items():
            expected_slot = expected[node]["input"][slot_name]
            assert len(slot) == 3
            assert slot["env_var"] == expected_slot["env_var"]
            assert slot["validator"] == expected_slot["validator"]
            assert slot["filepaths"] == [str(file) for file in expected_slot["files"]]
        # for slot_name, expected_slot in expected_slots["output"].items():
        for slot_name, slot in output_slots.items():
            expected_slot = expected[node]["output"][slot_name]
            assert len(slot) == 1
            assert slot["filepaths"] == [str(file) for file in expected_slot["filepaths"]]


def test__deduplicate_input_slots() -> None:
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
    condensed_slots = PipelineGraph._deduplicate_input_slots(input_slots, filepaths_by_slot)
    for slot_name, expected_slot in expected.items():
        slot = condensed_slots[slot_name]
        assert slot["env_var"] == expected_slot["env_var"]
        assert slot["validator"] == expected_slot["validator"]
        assert slot["filepaths"] == expected_slot["filepaths"]


@pytest.mark.parametrize(
    "duplicate_slot, error_msg",
    [
        (
            InputSlot(
                "step_1_main_input",
                env_var="DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
                validator=validate_input_file_dummy,
            ),
            "have different env_var values",
        ),
        (
            InputSlot(
                "step_1_main_input",
                env_var="DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                validator=lambda x: None,
            ),
            "have different validator values",
        ),
    ],
)
def test__deduplicate_input_slots_raises(duplicate_slot: InputSlot, error_msg: str) -> None:
    input_slots = [
        InputSlot(
            "step_1_main_input",
            "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            validate_input_file_dummy,
        ),
        duplicate_slot,
    ]
    filepaths_by_slot = [
        ["file1", "file2"],
        ["file3", "file4"],
    ]
    with pytest.raises(ValueError, match=error_msg):
        PipelineGraph._deduplicate_input_slots(input_slots, filepaths_by_slot)


def test__deduplicate_output_slots() -> None:
    output_slots = [
        OutputSlot("step_1_main_output"),
        OutputSlot("step_1_main_output"),
        OutputSlot("step_2_main_output"),
        OutputSlot("step_2_main_output"),
    ]
    filepaths_by_slot = [
        ["file1", "file2"],
        ["file3", "file4"],
        ["common_file1", "common_file2"],
        ["new_file", "common_file1", "common_file2"],
    ]
    expected = {
        "step_1_main_output": {
            "filepaths": ["file1", "file2", "file3", "file4"],
        },
        "step_2_main_output": {
            "filepaths": ["common_file1", "common_file2", "new_file"],
        },
    }
    condensed_slots = PipelineGraph._deduplicate_output_slots(output_slots, filepaths_by_slot)
    for slot_name, expected_slot in expected.items():
        slot = condensed_slots[slot_name]
        assert slot["filepaths"] == expected_slot["filepaths"]


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
            ["intermediate/step_3_step_3_main_input_split/{chunk}/result.parquet"],
            ["intermediate/step_3_python_pandas/{chunk}/result.parquet"],
        ),
        "step_4_python_pandas": (
            [
                f"{test_dir}/input_data1/file1.csv",
                f"{test_dir}/input_data2/file2.csv",
                "intermediate/step_3_aggregate/result.parquet",
            ],
            ["intermediate/step_4_python_pandas/result.parquet"],
        ),
    }
    pipeline_graph = PipelineGraph(default_config)
    for node, (expected_input_files, expected_output_files) in expected.items():
        input_files, output_files = pipeline_graph.get_io_filepaths(node)
        assert input_files == expected_input_files
        assert output_files == expected_output_files


@pytest.mark.parametrize("requires_spark", [True, False])
def test_spark_is_required(default_config_params, requires_spark):
    config_params = default_config_params
    if requires_spark:
        # Change step 1's implementation to python_pyspark
        config_params["pipeline"]["steps"]["step_1"]["implementation"][
            "name"
        ] = "step_1_python_pyspark"
    config = Config(config_params, schema_name="development")
    pipeline_graph = PipelineGraph(config)
    assert pipeline_graph.spark_is_required == requires_spark


def test_get_whether_auto_parallel(default_config_params):
    config_params = default_config_params
    config = Config(config_params, schema_name="development")
    pipeline_graph = PipelineGraph(config)
    for node in pipeline_graph.implementation_nodes:
        if node == "step_3_python_pandas":
            assert pipeline_graph.get_whether_auto_parallel(node)
        else:
            assert not pipeline_graph.get_whether_auto_parallel(node)


@pytest.mark.parametrize("any_auto_parallel", [True, False])
def test_any_auto_parallel(default_config_params, any_auto_parallel):
    config = Config(default_config_params, schema_name="development")
    pipeline_graph = PipelineGraph(config, freeze=False)
    if not any_auto_parallel:
        pipeline_graph.remove_node("step_3_python_pandas")
    assert pipeline_graph.any_auto_parallel == any_auto_parallel


def test_merge_combined_implementations(
    default_config_params, test_dir, unit_test_specifications_dir
) -> None:
    config_params = default_config_params
    # combine steps 1 and 2
    config_params["pipeline"] = load_yaml(
        f"{unit_test_specifications_dir}/pipeline_combine_two_steps.yaml"
    )
    pipeline_graph = PipelineGraph(Config(config_params, schema_name="development"))
    expected_nodes = {
        "input_data",
        "step_1_2",
        "step_3_step_3_main_input_split",
        "step_3_python_pandas",
        "step_3_aggregate",
        "step_4_python_pandas",
        "results",
    }
    expected_edges = {
        ("input_data", "step_1_2"): {
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
        ("step_1_2", "step_3_step_3_main_input_split"): {
            "input_slot_name": "step_3_main_input",
            "output_slot_name": "step_2_main_output",
            "validator": None,
            "env_var": None,
            "filepaths": (Path("intermediate/step_1_2/result.parquet"),),
        },
        ("step_3_step_3_main_input_split", "step_3_python_pandas"): {
            "input_slot_name": "step_3_main_input",
            "output_slot_name": "step_3_step_3_main_input_split_main_output",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": (
                Path("intermediate/step_3_step_3_main_input_split/{chunk}/result.parquet"),
            ),
        },
        ("step_3_python_pandas", "step_3_aggregate"): {
            "input_slot_name": "step_3_aggregate_main_input",
            "output_slot_name": "step_3_main_output",
            "validator": None,
            "env_var": None,
            "filepaths": (Path("intermediate/step_3_python_pandas/{chunk}/result.parquet"),),
        },
        ("step_3_aggregate", "step_4_python_pandas"): {
            "input_slot_name": "step_4_main_input",
            "output_slot_name": "step_3_main_output",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": (Path("intermediate/step_3_aggregate/result.parquet"),),
        },
        ("step_4_python_pandas", "results"): {
            "input_slot_name": "result",
            "output_slot_name": "step_4_main_output",
            "validator": validate_input_file_dummy,
            "env_var": None,
            "filepaths": (Path("intermediate/step_4_python_pandas/result.parquet"),),
        },
    }
    _check_nodes_and_edges(pipeline_graph, expected_nodes, expected_edges)


def test_merge_combined_implementations_iteration(
    default_config_params, test_dir, unit_test_specifications_dir
) -> None:
    config_params = default_config_params
    config_params["pipeline"] = load_yaml(
        f"{unit_test_specifications_dir}/pipeline_combine_with_iteration.yaml"
    )
    config = Config(config_params, "combine_with_iteration")
    pipeline_graph = PipelineGraph(config)

    expected_nodes = {
        "input_data",
        "step_1_loop_1_step_1_python_pandas",
        "step_1_2",
        "results",
    }
    expected_edges = {
        ("input_data", "step_1_loop_1_step_1_python_pandas"): {
            "input_slot_name": "step_1_main_input",
            "output_slot_name": "all",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": (
                Path(f"{test_dir}/input_data1/file1.csv"),
                Path(f"{test_dir}/input_data2/file2.csv"),
            ),
        },
        ("step_1_loop_1_step_1_python_pandas", "step_1_2"): {
            "input_slot_name": "step_1_main_input",
            "output_slot_name": "step_1_main_output",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": (
                Path("intermediate/step_1_loop_1_step_1_python_pandas/result.parquet"),
            ),
        },
        ("step_1_2", "results"): {
            "input_slot_name": "result",
            "output_slot_name": "step_2_main_output",
            "validator": validate_input_file_dummy,
            "env_var": None,
            "filepaths": (Path("intermediate/step_1_2/result.parquet"),),
        },
    }
    _check_nodes_and_edges(pipeline_graph, expected_nodes, expected_edges)


def test_merge_combined_implementations_parallel(
    default_config_params, test_dir, unit_test_specifications_dir
) -> None:
    config_params = default_config_params
    config_params["pipeline"] = load_yaml(
        f"{unit_test_specifications_dir}/pipeline_combine_with_parallel.yaml"
    )
    pipeline_graph = PipelineGraph(Config(config_params, schema_name="development"))
    expected_nodes = {
        "input_data",
        "step_1_clone_1_step_1_python_pandas",
        "step_1_clone_2_step_1_python_pandas",
        "steps_1_and_2_combined",
        "step_3_step_3_main_input_split",
        "step_3_python_pandas",
        "step_3_aggregate",
        "step_4_python_pandas",
        "results",
    }
    expected_edges = {
        ("input_data", "step_1_clone_1_step_1_python_pandas"): {
            "input_slot_name": "step_1_main_input",
            "output_slot_name": "all",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": (
                Path(f"{test_dir}/input_data1/file1.csv"),
                Path(f"{test_dir}/input_data2/file2.csv"),
            ),
        },
        ("input_data", "step_1_clone_2_step_1_python_pandas"): {
            "input_slot_name": "step_1_main_input",
            "output_slot_name": "all",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": (
                Path(f"{test_dir}/input_data1/file1.csv"),
                Path(f"{test_dir}/input_data2/file2.csv"),
            ),
        },
        ("input_data", "steps_1_and_2_combined"): {
            "input_slot_name": "step_1_step_1_main_input",
            "output_slot_name": "all",
            "validator": validate_input_file_dummy,
            "env_var": "STEP_1_DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
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
        ("step_1_clone_1_step_1_python_pandas", "steps_1_and_2_combined"): {
            "input_slot_name": "step_2_step_2_main_input",
            "output_slot_name": "step_1_main_output",
            "validator": validate_input_file_dummy,
            "env_var": "STEP_2_DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": (
                Path("intermediate/step_1_clone_1_step_1_python_pandas/result.parquet"),
            ),
        },
        ("step_1_clone_2_step_1_python_pandas", "steps_1_and_2_combined"): {
            "input_slot_name": "step_2_step_2_main_input",
            "output_slot_name": "step_1_main_output",
            "validator": validate_input_file_dummy,
            "env_var": "STEP_2_DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": (
                Path("intermediate/step_1_clone_2_step_1_python_pandas/result.parquet"),
            ),
        },
        ("steps_1_and_2_combined", "step_3_step_3_main_input_split"): {
            "input_slot_name": "step_3_main_input",
            "output_slot_name": "step_2_main_output",
            "validator": None,
            "env_var": None,
            "filepaths": (Path("intermediate/steps_1_and_2_combined/result.parquet"),),
        },
        ("step_3_step_3_main_input_split", "step_3_python_pandas"): {
            "input_slot_name": "step_3_main_input",
            "output_slot_name": "step_3_step_3_main_input_split_main_output",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": (
                Path("intermediate/step_3_step_3_main_input_split/{chunk}/result.parquet"),
            ),
        },
        ("step_3_python_pandas", "step_3_aggregate"): {
            "input_slot_name": "step_3_aggregate_main_input",
            "output_slot_name": "step_3_main_output",
            "validator": None,
            "env_var": None,
            "filepaths": (Path("intermediate/step_3_python_pandas/{chunk}/result.parquet"),),
        },
        ("step_3_aggregate", "step_4_python_pandas"): {
            "input_slot_name": "step_4_main_input",
            "output_slot_name": "step_3_main_output",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": (Path("intermediate/step_3_aggregate/result.parquet"),),
        },
        ("step_4_python_pandas", "results"): {
            "input_slot_name": "result",
            "output_slot_name": "step_4_main_output",
            "validator": validate_input_file_dummy,
            "env_var": None,
            "filepaths": (Path("intermediate/step_4_python_pandas/result.parquet"),),
        },
    }
    _check_nodes_and_edges(pipeline_graph, expected_nodes, expected_edges)


@pytest.mark.parametrize(
    "problem_key, error_msg, use_custom_schema",
    [
        (
            "combine_with_iteration_cycle",
            "The pipeline graph contains a cycle after combining implementations: [('step_1_2', 'step_1_loop_2_step_1_python_pandas', 0), "
            "('step_1_loop_2_step_1_python_pandas', 'step_1_2', 0)]",
            True,
        ),
        (
            "combine_with_extra_node",
            "Pipeline configuration nodes ['step_1', 'step_2', 'step_3'] do not match metadata steps ['step_1', 'step_2'].",
            True,
        ),
        (
            "combine_with_missing_node",
            "Pipeline configuration nodes ['step_4'] do not match metadata steps ['step_3', 'step_4'].",
            False,
        ),
        (
            "combine_bad_topology",
            "Pipeline configuration nodes ['step_1b', 'step_1a'] are not topologically consistent with the intended implementations for ['step_1a', 'step_1b']:\nThere is a path from successor step_1b to predecessor step_1a.",
            True,
        ),
        (
            "combine_bad_implementation_names",
            "Pipeline configuration nodes ['step_2', 'step_4'] do not match metadata steps ['step_3', 'step_4'].",
            False,
        ),
    ],
)
def test_bad_combined_configuration_raises(
    problem_key,
    error_msg,
    use_custom_schema,
    default_config_params,
    unit_test_specifications_dir,
) -> None:
    config_params = default_config_params
    config_params["pipeline"] = load_yaml(
        f"{unit_test_specifications_dir}/pipeline_{problem_key}.yaml"
    )
    with pytest.raises(ValueError, match=re.escape(error_msg)):
        if use_custom_schema:
            PipelineGraph(Config(config_params, problem_key))
        else:
            PipelineGraph(Config(config_params, schema_name="development"))


def test_nested_templated_steps(
    default_config_params, test_dir, unit_test_specifications_dir
) -> None:
    config_params = default_config_params
    config_params["pipeline"] = load_yaml(
        f"{unit_test_specifications_dir}/pipeline_nested_templated_steps.yaml"
    )
    # Ensure that Config instantiates without raising an exception
    pipeline_graph = PipelineGraph(Config(config_params, "nested_templated_steps"))
    expected_nodes = {
        "input_data",
        "step_1_loop_1_step_1_loop_1_clone_1_step_1_python_pandas",
        "step_1_loop_1_step_1_loop_1_clone_2_step_1_python_pandas",
        "step_1_loop_2_step_1_loop_2_clone_1_step_1_python_pandas",
        "step_1_loop_3_step_1_loop_3_step_1a_step_1a_python_pandas",
        "step_1_loop_3_step_1_loop_3_step_1b_step_1b_python_pandas",
        "results",
    }
    expected_edges = {
        ("input_data", "step_1_loop_1_step_1_loop_1_clone_1_step_1_python_pandas"): {
            "input_slot_name": "step_1_main_input",
            "output_slot_name": "all",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": (
                Path(f"{test_dir}/input_data1/file1.csv"),
                Path(f"{test_dir}/input_data2/file2.csv"),
            ),
        },
        ("input_data", "step_1_loop_1_step_1_loop_1_clone_2_step_1_python_pandas"): {
            "input_slot_name": "step_1_main_input",
            "output_slot_name": "all",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": (
                Path(f"{test_dir}/input_data1/file1.csv"),
                Path(f"{test_dir}/input_data2/file2.csv"),
            ),
        },
        (
            "step_1_loop_1_step_1_loop_1_clone_1_step_1_python_pandas",
            "step_1_loop_2_step_1_loop_2_clone_1_step_1_python_pandas",
        ): {
            "input_slot_name": "step_1_main_input",
            "output_slot_name": "step_1_main_output",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": (
                Path(
                    "intermediate/step_1_loop_1_step_1_loop_1_clone_1_step_1_python_pandas/result.parquet"
                ),
            ),
        },
        (
            "step_1_loop_1_step_1_loop_1_clone_2_step_1_python_pandas",
            "step_1_loop_2_step_1_loop_2_clone_1_step_1_python_pandas",
        ): {
            "input_slot_name": "step_1_main_input",
            "output_slot_name": "step_1_main_output",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": (
                Path(
                    "intermediate/step_1_loop_1_step_1_loop_1_clone_2_step_1_python_pandas/result.parquet"
                ),
            ),
        },
        (
            "step_1_loop_2_step_1_loop_2_clone_1_step_1_python_pandas",
            "step_1_loop_3_step_1_loop_3_step_1a_step_1a_python_pandas",
        ): {
            "input_slot_name": "step_1a_main_input",
            "output_slot_name": "step_1_main_output",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": (
                Path(
                    "intermediate/step_1_loop_2_step_1_loop_2_clone_1_step_1_python_pandas/result.parquet"
                ),
            ),
        },
        (
            "step_1_loop_3_step_1_loop_3_step_1a_step_1a_python_pandas",
            "step_1_loop_3_step_1_loop_3_step_1b_step_1b_python_pandas",
        ): {
            "input_slot_name": "step_1b_main_input",
            "output_slot_name": "step_1a_main_output",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": (
                Path(
                    "intermediate/step_1_loop_3_step_1_loop_3_step_1a_step_1a_python_pandas/result.parquet"
                ),
            ),
        },
        ("step_1_loop_3_step_1_loop_3_step_1b_step_1b_python_pandas", "results"): {
            "input_slot_name": "result",
            "output_slot_name": "step_1b_main_output",
            "validator": validate_input_file_dummy,
            "env_var": None,
            "filepaths": (
                Path(
                    "intermediate/step_1_loop_3_step_1_loop_3_step_1b_step_1b_python_pandas/result.parquet"
                ),
            ),
        },
    }
    _check_nodes_and_edges(pipeline_graph, expected_nodes, expected_edges)


####################
# Helper functions #
####################


def _check_nodes_and_edges(pipeline_graph, expected_nodes, expected_edges):
    assert set(pipeline_graph.nodes) == set(expected_nodes)
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
            str(file) for file in expected_edges[(source, sink)]["filepaths"]
        )
