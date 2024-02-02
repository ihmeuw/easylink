from linker.step import Step, StepInput


def test_step_instantiation():
    step = Step("foo", validate_output=lambda x: x)
    assert step.name == "foo"
    assert step.validate_output(True)
    assert not step.validate_output(False)


def test_step_input():
    input_params = {
        "env_var": "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS",
        "container_dir_name": "/input_data/main_input",
        "host_filepaths": [
            "/mnt/team/simulation_science/priv/engineering/er_ecosystem/sample_data/dummy/input_file_1.parquet"
        ],
        "prev_output": False,
    }

    step_input = StepInput(**input_params)
    assert step_input.container_paths == ["/input_data/main_input/input_file_1.parquet"]
    assert step_input.bindings == {
        "/mnt/team/simulation_science/priv/engineering/er_ecosystem/sample_data/dummy/input_file_1.parquet": "/input_data/main_input/input_file_1.parquet"
    }
    step_input.add_bindings(["/foo/bar"])
    assert step_input.host_filepaths == [
        "/mnt/team/simulation_science/priv/engineering/er_ecosystem/sample_data/dummy/input_file_1.parquet",
        "/foo/bar",
    ]
    assert step_input.container_paths == [
        "/input_data/main_input/input_file_1.parquet",
        "/input_data/main_input/bar",
    ]
    assert step_input.bindings == {
        "/mnt/team/simulation_science/priv/engineering/er_ecosystem/sample_data/dummy/input_file_1.parquet": "/input_data/main_input/input_file_1.parquet",
        "/foo/bar": "/input_data/main_input/bar",
    }


def test_add_bindings_from_prev():
    step = Step(
        "foo",
        validate_output=lambda x: x,
        inputs={
            "DUMMY_CONTAINER_MAIN_INPUT_FILE_PATHS": {
                "container_dir_name": "/input_data/main_input",
                "host_filepaths": [
                    "/mnt/team/simulation_science/priv/engineering/er_ecosystem/sample_data/dummy/input_file_1.parquet"
                ],
                "prev_output": True,
            }
        },
    )
    step.add_bindings_from_prev(["/foo/bar"])
    assert set(step.inputs[0].host_filepaths) == {
        "/foo/bar",
        "/mnt/team/simulation_science/priv/engineering/er_ecosystem/sample_data/dummy/input_file_1.parquet",
    }
    assert set(step.inputs[0].container_paths) == {
        "/input_data/main_input/bar",
        "/input_data/main_input/input_file_1.parquet",
    }
    for k, v in {
        "/foo/bar": "/input_data/main_input/bar",
        "/mnt/team/simulation_science/priv/engineering/er_ecosystem/sample_data/dummy/input_file_1.parquet": "/input_data/main_input/input_file_1.parquet",
    }.items():
        assert step.inputs[0].bindings[k] == v
