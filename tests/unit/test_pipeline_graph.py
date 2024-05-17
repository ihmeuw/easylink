from easylink.pipeline_graph import PipelineGraph


def test__create_graph(default_config, test_dir):
    pipeline_graph = PipelineGraph(default_config)
    assert set(pipeline_graph.nodes) == {
        "input_data",
        "1_step_1",
        "2_step_2",
        "3_step_3",
        "4_step_4",
        "results",
    }
    expected_edges = [
        (
            "input_data",
            "1_step_1",
            {
                "files": [
                    f"{test_dir}/input_data1/file1.csv",
                    f"{test_dir}/input_data2/file2.csv",
                ]
            },
        ),
        ("1_step_1", "2_step_2", {"files": ["intermediate/1_step_1/result.parquet"]}),
        ("2_step_2", "3_step_3", {"files": ["intermediate/2_step_2/result.parquet"]}),
        ("3_step_3", "4_step_4", {"files": ["intermediate/3_step_3/result.parquet"]}),
        ("4_step_4", "results", {"files": ["result.parquet"]}),
    ]
    pipeline_edges = pipeline_graph.edges(data=True)

    for edge in pipeline_edges:
        assert edge in expected_edges
    for edge in expected_edges:
        assert edge in pipeline_edges


def test_implementations(default_config):
    pipeline_graph = PipelineGraph(default_config)
    implementation_names = [
        implementation.name for implementation in pipeline_graph.implementations
    ]
    assert implementation_names == [
        "step_1_python_pandas",
        "step_2_python_pandas",
        "step_3_python_pandas",
        "step_4_python_pandas",
    ]


def test_implementation_nodes(default_config):
    pipeline_graph = PipelineGraph(default_config)
    assert pipeline_graph.implementation_nodes == [
        "1_step_1",
        "2_step_2",
        "3_step_3",
        "4_step_4",
    ]


def test_get_input_output_files(default_config, test_dir):
    expected = {
        "1_step_1": (
            [
                f"{test_dir}/input_data1/file1.csv",
                f"{test_dir}/input_data2/file2.csv",
            ],
            ["intermediate/1_step_1/result.parquet"],
        ),
        "2_step_2": (
            ["intermediate/1_step_1/result.parquet"],
            ["intermediate/2_step_2/result.parquet"],
        ),
        "4_step_4": (["intermediate/3_step_3/result.parquet"], ["result.parquet"]),
    }
    pipeline_graph = PipelineGraph(default_config)
    for node, (expected_input_files, expected_output_files) in expected.items():
        input_files, output_files = pipeline_graph.get_input_output_files(node)
        assert input_files == expected_input_files
        assert output_files == expected_output_files
