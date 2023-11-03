import pytest
import yaml

ENV_CONFIG_DICT = {
    "computing_environment": "foo",
    "baz": {
        "qux",
        "quux",
    },
}

PIPELINE_CONFIG_DICT = {
    "steps": {
        "pvs_like_case_study": {
            "implementation": "pvs_like_python",
        },
    },
}


@pytest.fixture(scope="session")
def config_path(tmpdir_factory):
    tmp_path = tmpdir_factory.getbasetemp()
    # pipeline
    with open(f"{str(tmp_path)}/pipeline.yaml", "w") as file:
        yaml.dump(PIPELINE_CONFIG_DICT, file, sort_keys=False)
    # environment
    with open(f"{str(tmp_path)}/environment.yaml", "w") as file:
        yaml.dump(ENV_CONFIG_DICT, file, sort_keys=False)
    # input
    input_dir1 = tmp_path.mkdir("input_data1")
    input_dir2 = tmp_path.mkdir("input_data2")
    with open(f"{str(input_dir1)}/file1", "w") as file:
        file.write("")
    with open(f"{str(input_dir2)}/file2", "w") as file:
        file.write("")
    with open(f"{tmp_path}/input_data.yaml", "w") as file:
        yaml.dump(
            {
                "foo": str(input_dir1 / "file1"),
                "bar": str(input_dir2 / "file2"),
            },
            file,
            sort_keys=False,
        )

    return str(tmp_path)
