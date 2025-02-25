import re
from pathlib import Path

import pytest

from easylink.configuration import Config
from easylink.graph_components import InputSlot, OutputSlot
from easylink.pipeline_graph import PipelineGraph
from easylink.pipeline_schema import PipelineSchema
from easylink.pipeline_schema_constants import TESTING_SCHEMA_PARAMS
from easylink.utilities.aggregator_utils import concatenate_datasets
from easylink.utilities.data_utils import load_yaml
from easylink.utilities.splitter_utils import split_data_by_size
from easylink.utilities.validation_utils import validate_input_file_dummy


def test__create_graph(default_config: Config, test_dir: str) -> None:
    pipeline_graph = PipelineGraph(default_config)
    expected_nodes = {
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
    check_nodes_and_edges(pipeline_graph, expected_nodes, expected_edges)


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
                ("step_a", InputSlot("foo", env_var="bar", validator=None)),
                ("step_b", InputSlot("baz", env_var="spam", validator=None)),
            },
        ),
        (
            "input_slot",
            True,
            {
                ("step_a", InputSlot("foo", env_var="bar", validator=None)),
                ("step_b", InputSlot("foo", env_var="spam", validator=None)),
            },
        ),
        (
            "input_slot",
            True,
            {
                ("step_a", InputSlot("foo", env_var="bar", validator=None)),
                ("step_b", InputSlot("baz", env_var="bar", validator=None)),
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


def test_get_io_slots(default_config: Config, test_dir: str) -> None:
    expected = {
        "step_1_python_pandas": {
            "input": {
                "step_1_main_input": {
                    "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    "validator": validate_input_file_dummy,
                    "files": [
                        Path(f"{test_dir}/input_data1/file1.csv"),
                        Path(f"{test_dir}/input_data2/file2.csv"),
                    ],
                    "splitter": None,
                },
            },
            "output": {
                "step_1_main_output": {
                    "filepaths": [Path("intermediate/step_1_python_pandas/result.parquet")],
                    "aggregator": None,
                },
            },
        },
        "step_2_python_pandas": {
            "input": {
                "step_2_main_input": {
                    "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    "validator": validate_input_file_dummy,
                    "files": [Path("intermediate/step_1_python_pandas/result.parquet")],
                    "splitter": None,
                },
            },
            "output": {
                "step_2_main_output": {
                    "filepaths": [Path("intermediate/step_2_python_pandas/result.parquet")],
                    "aggregator": None,
                },
            },
        },
        "step_3_python_pandas": {
            "input": {
                "step_3_main_input": {
                    "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    "validator": validate_input_file_dummy,
                    "files": [Path("intermediate/step_2_python_pandas/result.parquet")],
                    "splitter": split_data_by_size,
                },
            },
            "output": {
                "step_3_main_output": {
                    "filepaths": [Path("intermediate/step_3_python_pandas/result.parquet")],
                    "aggregator": concatenate_datasets,
                },
            },
        },
        "step_4_python_pandas": {
            "input": {
                "step_4_main_input": {
                    "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                    "validator": validate_input_file_dummy,
                    "files": [Path("intermediate/step_3_python_pandas/result.parquet")],
                    "splitter": None,
                },
            },
            "output": {
                "step_4_main_output": {
                    "filepaths": [Path("intermediate/step_4_python_pandas/result.parquet")],
                    "aggregator": None,
                },
            },
        },
    }
    pipeline_graph = PipelineGraph(default_config)
    for node, expected_slots in expected.items():
        input_slots, output_slots = pipeline_graph.get_io_slot_attributes(node)
        for slot_name, expected_slot in expected_slots["input"].items():
            slot = input_slots[slot_name]
            assert len(slot) == 4
            assert slot["env_var"] == expected_slot["env_var"]
            assert slot["validator"] == expected_slot["validator"]
            assert slot["filepaths"] == [str(file) for file in expected_slot["files"]]
            assert slot["splitter"] == expected_slot["splitter"]
        for slot_name, expected_slot in expected_slots["output"].items():
            slot = output_slots[slot_name]
            assert len(slot) == 2
            assert slot["filepaths"] == [str(file) for file in expected_slot["filepaths"]]
            assert slot["aggregator"] == expected_slot["aggregator"]


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
    condensed_slots = PipelineGraph._deduplicate_input_slots(input_slots, filepaths_by_slot)
    for slot_name, expected_slot in expected.items():
        slot = condensed_slots[slot_name]
        assert slot["env_var"] == expected_slot["env_var"]
        assert slot["validator"] == expected_slot["validator"]
        assert slot["filepaths"] == expected_slot["filepaths"]


def test_condense_input_slots_duplicate_slots_raises() -> None:
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
        PipelineGraph._deduplicate_input_slots(input_slots, filepaths_by_slot)


def test_condense_input_slots_duplicate_slots_raises() -> None:
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
        PipelineGraph._deduplicate_input_slots(input_slots, filepaths_by_slot)


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
    config = Config(config_params)
    pipeline_graph = PipelineGraph(config)
    assert pipeline_graph.spark_is_required == requires_spark


def test_get_whether_embarrassingly_parallel(default_config_params):
    config_params = default_config_params
    config = Config(config_params)
    pipeline_graph = PipelineGraph(config)
    nodes = [node for node in pipeline_graph.nodes if node not in ["input_data", "results"]]
    for node in nodes:
        if node == "step_3_python_pandas":
            assert pipeline_graph.get_whether_embarrassingly_parallel(node)
        else:
            assert not pipeline_graph.get_whether_embarrassingly_parallel(node)


@pytest.mark.parametrize("any_embarrassingly_parallel", [True, False])
def test_any_embarrassingly_parallel(default_config_params, any_embarrassingly_parallel):
    config = Config(default_config_params)
    pipeline_graph = PipelineGraph(config, freeze=False)
    if not any_embarrassingly_parallel:
        pipeline_graph.remove_node("step_3_python_pandas")
    assert pipeline_graph.any_embarrassingly_parallel == any_embarrassingly_parallel


def test_merge_combined_implementations(
    default_config_params, test_dir, unit_test_specifications_dir
) -> None:
    config_params = default_config_params
    # combine steps 1 and 2
    config_params["pipeline"] = load_yaml(
        f"{unit_test_specifications_dir}/pipeline_combine_two_steps.yaml"
    )
    pipeline_graph = PipelineGraph(Config(config_params))
    expected_nodes = {
        "input_data",
        "step_1_2",
        "step_3_python_pandas",
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
        ("step_1_2", "step_3_python_pandas"): {
            "input_slot_name": "step_3_main_input",
            "output_slot_name": "step_2_main_output",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": (Path("intermediate/step_1_2/result.parquet"),),
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
    check_nodes_and_edges(pipeline_graph, expected_nodes, expected_edges)


def test_merge_combined_implementations_iteration(
    default_config_params, test_dir, unit_test_specifications_dir
) -> None:
    config_params = default_config_params
    config_params["pipeline"] = load_yaml(
        f"{unit_test_specifications_dir}/pipeline_combine_with_iteration.yaml"
    )
    schema = PipelineSchema(
        "combine_with_iteration", *TESTING_SCHEMA_PARAMS["combine_with_iteration"]
    )
    config = Config(config_params, schema)
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
    check_nodes_and_edges(pipeline_graph, expected_nodes, expected_edges)


def test_merge_combined_implementations_parallel(
    default_config_params, test_dir, unit_test_specifications_dir
) -> None:
    config_params = default_config_params
    config_params["pipeline"] = load_yaml(
        f"{unit_test_specifications_dir}/pipeline_combine_with_parallel.yaml"
    )
    pipeline_graph = PipelineGraph(Config(config_params))
    expected_nodes = {
        "input_data",
        "step_1_parallel_split_1_step_1_python_pandas",
        "step_1_parallel_split_2_step_1_python_pandas",
        "steps_1_and_2_combined",
        "step_3_python_pandas",
        "step_4_python_pandas",
        "results",
    }
    expected_edges = {
        ("input_data", "step_1_parallel_split_1_step_1_python_pandas"): {
            "input_slot_name": "step_1_main_input",
            "output_slot_name": "all",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": (
                Path(f"{test_dir}/input_data1/file1.csv"),
                Path(f"{test_dir}/input_data2/file2.csv"),
            ),
        },
        ("input_data", "step_1_parallel_split_2_step_1_python_pandas"): {
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
        ("step_1_parallel_split_1_step_1_python_pandas", "steps_1_and_2_combined"): {
            "input_slot_name": "step_2_step_2_main_input",
            "output_slot_name": "step_1_main_output",
            "validator": validate_input_file_dummy,
            "env_var": "STEP_2_DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": (
                Path(
                    "intermediate/step_1_parallel_split_1_step_1_python_pandas/result.parquet"
                ),
            ),
        },
        ("step_1_parallel_split_2_step_1_python_pandas", "steps_1_and_2_combined"): {
            "input_slot_name": "step_2_step_2_main_input",
            "output_slot_name": "step_1_main_output",
            "validator": validate_input_file_dummy,
            "env_var": "STEP_2_DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": (
                Path(
                    "intermediate/step_1_parallel_split_2_step_1_python_pandas/result.parquet"
                ),
            ),
        },
        ("steps_1_and_2_combined", "step_3_python_pandas"): {
            "input_slot_name": "step_3_main_input",
            "output_slot_name": "step_2_main_output",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": (Path("intermediate/steps_1_and_2_combined/result.parquet"),),
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
    check_nodes_and_edges(pipeline_graph, expected_nodes, expected_edges)


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
            schema = PipelineSchema(problem_key, *TESTING_SCHEMA_PARAMS[problem_key])
            PipelineGraph(Config(config_params, schema))
        else:
            PipelineGraph(Config(config_params))


def test_nested_templated_steps(
    default_config_params, test_dir, unit_test_specifications_dir
) -> None:
    config_params = default_config_params
    config_params["pipeline"] = load_yaml(
        f"{unit_test_specifications_dir}/pipeline_nested_templated_steps.yaml"
    )
    # Need a custom schema to allow nested TemplatedSteps
    schema = PipelineSchema(
        "nested_templated_steps", *TESTING_SCHEMA_PARAMS["nested_templated_steps"]
    )
    # Ensure that Config instantiates without raising an exception
    pipeline_graph = PipelineGraph(Config(config_params, schema))
    expected_nodes = {
        "input_data",
        "step_1_loop_1_step_1_loop_1_parallel_split_1_step_1_python_pandas",
        "step_1_loop_1_step_1_loop_1_parallel_split_2_step_1_python_pandas",
        "step_1_loop_2_step_1_loop_2_parallel_split_1_step_1_python_pandas",
        "step_1_loop_3_step_1_loop_3_step_1a_step_1a_python_pandas",
        "step_1_loop_3_step_1_loop_3_step_1b_step_1b_python_pandas",
        "results",
    }
    expected_edges = {
        ("input_data", "step_1_loop_1_step_1_loop_1_parallel_split_1_step_1_python_pandas"): {
            "input_slot_name": "step_1_main_input",
            "output_slot_name": "all",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": (
                Path(f"{test_dir}/input_data1/file1.csv"),
                Path(f"{test_dir}/input_data2/file2.csv"),
            ),
        },
        ("input_data", "step_1_loop_1_step_1_loop_1_parallel_split_2_step_1_python_pandas"): {
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
            "step_1_loop_1_step_1_loop_1_parallel_split_1_step_1_python_pandas",
            "step_1_loop_2_step_1_loop_2_parallel_split_1_step_1_python_pandas",
        ): {
            "input_slot_name": "step_1_main_input",
            "output_slot_name": "step_1_main_output",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": (
                Path(
                    "intermediate/step_1_loop_1_step_1_loop_1_parallel_split_1_step_1_python_pandas/result.parquet"
                ),
            ),
        },
        (
            "step_1_loop_1_step_1_loop_1_parallel_split_2_step_1_python_pandas",
            "step_1_loop_2_step_1_loop_2_parallel_split_1_step_1_python_pandas",
        ): {
            "input_slot_name": "step_1_main_input",
            "output_slot_name": "step_1_main_output",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": (
                Path(
                    "intermediate/step_1_loop_1_step_1_loop_1_parallel_split_2_step_1_python_pandas/result.parquet"
                ),
            ),
        },
        (
            "step_1_loop_2_step_1_loop_2_parallel_split_1_step_1_python_pandas",
            "step_1_loop_3_step_1_loop_3_step_1a_step_1a_python_pandas",
        ): {
            "input_slot_name": "step_1a_main_input",
            "output_slot_name": "step_1_main_output",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": (
                Path(
                    "intermediate/step_1_loop_2_step_1_loop_2_parallel_split_1_step_1_python_pandas/result.parquet"
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
    check_nodes_and_edges(pipeline_graph, expected_nodes, expected_edges)


####################
# Helper functions #
####################


def check_nodes_and_edges(pipeline_graph, expected_nodes, expected_edges):
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
            [str(file) for file in expected_edges[(source, sink)]["filepaths"]]
        )
