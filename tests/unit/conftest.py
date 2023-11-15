import pytest
import yaml

from linker.configuration import Config

ENV_CONFIG_DICT = {
    "computing_environment": "foo",
    "baz": {
        "qux",
        "quux",
    },
}

PIPELINE_CONFIG_DICT = {
    "good": {
        "steps": {
            "pvs_like_case_study": {
                "implementation": "pvs_like_python",
            },
        },
    },
    "bad_step": {
        "steps": {
            "foo": {  # Not a supported step
                "implementation": "bar",
            },
        },
    },
}


@pytest.fixture(scope="session")
def test_dir(tmpdir_factory) -> str:
    tmp_path = tmpdir_factory.getbasetemp()
    # pipeline
    with open(f"{str(tmp_path)}/pipeline.yaml", "w") as file:
        yaml.dump(PIPELINE_CONFIG_DICT["good"], file, sort_keys=False)
    with open(f"{str(tmp_path)}/bad_step_pipeline.yaml", "w") as file:
        yaml.dump(PIPELINE_CONFIG_DICT["bad_step"], file, sort_keys=False)
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
    # steps/implementations
    good_implementation_dir = tmp_path.join(
        "steps/pvs_like_case_study/implementations/pvs_like_python/"
    ).ensure(dir=True)
    with open(f"{good_implementation_dir}/metadata.yaml", "w") as file:
        yaml.dump(
            {
                "step": "pvs_like_case_study",
                "image": {
                    "directory": "some/path/to/container/directory",
                    "filename": "pvs_like_python",
                },
            },
            file,
            sort_keys=False,
        )
    bad_step_implementation_dir = tmp_path.join("steps/foo/implementations/bar/").ensure(
        dir=True
    )
    with open(f"{bad_step_implementation_dir}/metadata.yaml", "w") as file:
        yaml.dump(
            {
                "step": "foo",
                "image": {
                    "directory": "some/path/to/container/directory",
                    "filename": "bar",
                },
            },
            file,
            sort_keys=False,
        )

    return str(tmp_path)


@pytest.fixture(scope="session")
def config(test_dir) -> Config:
    return Config(
        f"{test_dir}/pipeline.yaml",
        f"{test_dir}/input_data.yaml",
        f"{test_dir}/environment.yaml",
    )