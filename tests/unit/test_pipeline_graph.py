from easylink.pipeline_graph import PipelineGraph


def test__create_graph(default_config, test_dir):
    pipeline_graph = PipelineGraph(default_config)
    assert set(pipeline_graph.nodes) == {
        "input_data",
        "step_1_python_pandas",
        "step_2_python_pandas",
        "step_3_python_pandas",
        "step_4_python_pandas",
        "results",
    }
    expected_edges = [
        (
            "input_data",
            "step_1_python_pandas",
            {
                "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                "files": [
                    f"{test_dir}/input_data1/file1.csv",
                ],
            },
        ),
        (
            "input_data",
            "step_4_python_pandas",
            {
                "env_var": "DUMMY_CONTAINER_SECONDARY_INPUT_FILE_PATHS",
                "files": [
                    f"{test_dir}/input_data1/file1.csv",
                ],
            },
        ),
        (
            "step_1_python_pandas",
            "step_2_python_pandas",
            {
                "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                "files": ["intermediate/step_1_python_pandas/result.parquet"],
            },
        ),
        (
            "step_2_python_pandas",
            "step_3_python_pandas",
            {
                "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                "files": ["intermediate/step_2_python_pandas/result.parquet"],
            },
        ),
        (
            "step_3_python_pandas",
            "step_4_python_pandas",
            {
                "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                "files": ["intermediate/step_3_python_pandas/result.parquet"],
            },
        ),
        (
            "step_4_python_pandas",
            "results",
            {
                "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
                "files": ["results/step_4_python_pandas/result.parquet"],
            },
        ),
    ]
    assert list(pipeline_graph.edges(data=True)) == expected_edges


def test_implementations(default_config):
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


def test_get_input_output_files(default_config, test_dir):
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
            ["results/step_4_python_pandas/result.parquet"],
        ),
    }
    pipeline_graph = PipelineGraph(default_config)
    for node, (expected_input_files, expected_output_files) in expected.items():
        input_files, output_files = pipeline_graph.get_input_output_files(node)
        assert input_files == expected_input_files
        assert output_files == expected_output_files
