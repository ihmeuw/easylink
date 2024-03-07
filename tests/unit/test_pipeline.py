from linker.pipeline import Pipeline


def test__get_implementations(default_config, mocker):
    mocker.patch("linker.implementation.Implementation.validate", return_value={})
    pipeline = Pipeline(default_config)
    implementation_names = [
        implementation.name for implementation in pipeline.implementations
    ]
    assert implementation_names == ["step_1_python_pandas", "step_2_python_pandas"]
