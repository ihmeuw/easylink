from linker.step import Step, StepInput


def test_step_instantiation():
    step = Step("foo", validate_output=lambda x: x)
    assert step.name == "foo"
    assert step.validate_output(True)
    assert not step.validate_output(False)


def test_step_input():
    input_params = {
        "env_var": "foo",
        "container_dir_name": "/bar",
        "input_filenames": ["step1"],
        "prev_output": False,
    }

    step_input = StepInput(**input_params)
    assert step_input.env_var == "foo"
    assert step_input.container_dir_name == "/bar"
    assert step_input.input_filenames == ["step1"]
    assert step_input.prev_output is False
    assert step_input.host_filepaths == []


def test_add_bindings_from_prev():
    input_params = {
        "foo": {
            "container_dir_name": "/bar",
            "input_filenames": [],
            "prev_output": True,
        }
    }
    step = Step("step1", validate_output=lambda x: x, inputs=input_params)
    step.add_bindings_from_prev(["/previous_step/eggs.parquet"])
    assert step.inputs[0].host_filepaths == ["/previous_step/eggs.parquet"]
    assert step.inputs[0].container_paths == ["/bar/eggs.parquet"]
    assert step.inputs[0].bindings == {
        "/previous_step/eggs.parquet": "/bar/eggs.parquet",
    }
    
def test_add_input_filename_bindings():
    input_params = {
        "foo": {
            "container_dir_name": "/bar",
            "input_filenames": ["foo"],
            "host_filepaths": [],
            "prev_output": True,
        }
    }
    step = Step("step1", validate_output=lambda x: x, inputs=input_params)
    step.add_input_filename_bindings({"foo":"/baz/spam.csv"})
    assert step.inputs[0].host_filepaths == ["/baz/spam.csv"]
    assert step.inputs[0].container_paths == ["/bar/spam.csv"]
    assert step.inputs[0].bindings == {
        "/baz/spam.csv": "/bar/spam.csv",
    }
