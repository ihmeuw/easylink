from tempfile import TemporaryDirectory

from linker.runner import get_environment_args, get_singularity_args


def test_get_singularity_args(default_config, test_dir):
    with TemporaryDirectory() as bind_dir:
        assert (
            get_singularity_args(default_config.input_data, bind_dir)
            == f"--no-home --containall -B /tmp,"
            f"{bind_dir},"
            f"{test_dir}/input_data1/file1.csv,"
            f"{test_dir}/input_data2/file2.csv"
        )


def test_get_environment_args(default_config, test_dir):
    assert default_config.computing_environment == "local"
    assert get_environment_args(default_config, test_dir) == []
