from pathlib import Path

import pytest

from linker.utilities.singularity_utils import _build_cmd

# TODO: is there more we can do here?


def test__build_cmd():
    input_data = [Path("some/input/file1.csv"), Path("some/input/file2.csv")]
    results_dir = Path("some/results/dir")
    diagnostics_dir = Path("some/diagnostics/dir")
    container_path = Path("some/container/path.sif")
    cmd = _build_cmd(input_data, results_dir, diagnostics_dir, container_path)
    expected = (
        "singularity run --containall --no-home --bind /tmp:/tmp "
        "--bind some/results/dir:/results --bind some/diagnostics/dir:/diagnostics "
        "--bind some/input/file1.csv:/input_data/main_input_file1.csv "
        "--bind some/input/file2.csv:/input_data/main_input_file2.csv "
        "some/container/path.sif"
    )
    assert cmd == expected


@pytest.mark.skip(reason="Do we need to do this or just rely on e2e tests?")
def test__run_cmd(): ...


@pytest.mark.skip(reason="TODO [MIC-4730] clean up after singularity runs")
def test__clean(): ...
