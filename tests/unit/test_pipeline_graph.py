from pathlib import Path

import pytest

from easylink.configuration import Config
from easylink.graph_components import InputSlot, OutputSlot
from easylink.pipeline_graph import PipelineGraph
from easylink.utilities.validation_utils import validate_input_file_dummy
from tests.unit.conftest import COMBINED_IMPLEMENTATION_CONFIGS


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
        ] = "step_1_python_pyspark"
    config = Config(config_params)
    pipeline_graph = PipelineGraph(config)
    assert pipeline_graph.spark_is_required() == requires_spark


def test_merge_combined_implementations(default_config_params, test_dir) -> None:
    config_params = default_config_params
    # make step 3 and step 4 a combined implementations
    config_params["pipeline"] = COMBINED_IMPLEMENTATION_CONFIGS["two_steps"]
    pipeline_graph = PipelineGraph(Config(config_params))
    expected_nodes = {
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
    check_nodes_and_edges(pipeline_graph, expected_nodes, expected_edges)


def test_merge_combined_implementations_iteration(default_config_params, test_dir) -> None:
    config_params = default_config_params
    # make step 3 and step 4 a combined implementations
    config_params["pipeline"] = COMBINED_IMPLEMENTATION_CONFIGS["with_iteration"]
    pipeline_graph = PipelineGraph(Config(config_params))
    expected_nodes = {
        "input_data",
        "step_1_python_pandas",
        "step_2_python_pandas",
        "step_3_loop_1_step_3_python_pandas",
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
        ("step_2_python_pandas", "step_3_loop_1_step_3_python_pandas"): {
            "input_slot_name": "step_3_main_input",
            "output_slot_name": "step_2_main_output",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": (Path("intermediate/step_2_python_pandas/result.parquet"),),
        },
        ("step_3_loop_1_step_3_python_pandas", "step_3_4"): {
            "input_slot_name": "step_3_main_input",
            "output_slot_name": "step_3_main_output",
            "validator": validate_input_file_dummy,
            "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
            "filepaths": (
                Path("intermediate/step_3_loop_1_step_3_python_pandas/result.parquet"),
            ),
        },
        ("step_3_4", "results"): {
            "input_slot_name": "result",
            "output_slot_name": "step_4_main_output",
            "validator": validate_input_file_dummy,
            "env_var": None,
            "filepaths": (Path("intermediate/step_3_4/result.parquet"),),
        },
    }
    check_nodes_and_edges(pipeline_graph, expected_nodes, expected_edges)


def test_merge_combined_implementations_parallel(default_config_params, test_dir) -> None:
    config_params = default_config_params
    config_params["pipeline"] = COMBINED_IMPLEMENTATION_CONFIGS["with_parallel"]
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


def test_cycle_error(default_config_params) -> None:
    config_params = default_config_params
    config_params["pipeline"] = COMBINED_IMPLEMENTATION_CONFIGS["with_iteration_cycle"]
    with pytest.raises(ValueError, match=("The MultiDiGraph contains a cycle:")):
        PipelineGraph(Config(config_params))


def test_combined_extra_step(default_config_params):
    config_params = default_config_params
    config_params["pipeline"] = COMBINED_IMPLEMENTATION_CONFIGS["with_extra_node"]
    with pytest.raises(
        ValueError,
        match=r"Pipeline configuration nodes \['step_2', 'step_3', 'step_4'\] do not match metadata steps \['step_3', 'step_4'\].",
    ):
        PipelineGraph(Config(config_params))


def test_combined_missing_node(default_config_params):
    config_params = default_config_params
    config_params["pipeline"] = COMBINED_IMPLEMENTATION_CONFIGS["with_missing_node"]
    with pytest.raises(
        ValueError,
        match=r"Pipeline configuration nodes \['step_4'\] do not match metadata steps \['step_3', 'step_4'\].",
    ):
        PipelineGraph(Config(config_params))


# TODO MIC-5476: Add a test here when we have modularized pipeline schemas for testing
@pytest.mark.skip(reason="Not implemented")
def test_combined_bad_topology():
    pass


### Helper functions ###


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
