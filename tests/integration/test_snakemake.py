import tempfile
from pathlib import Path

import pytest

from linker.runner import main
from tests.conftest import SPECIFICATIONS_DIR


@pytest.mark.slow
def test_missing_results(mocker, caplog):
    """Test that the pipeline fails when a step is missing output files."""
    
    ## Mock implementation script call to wait 1s instead of running something
    mocker.patch(
        "linker.implementation.Implementation.script_cmd",
        new_callable=mocker.PropertyMock,
        return_value="sleep 1s",
    )
    with tempfile.TemporaryDirectory(dir="tests/integration/") as results_dir:
        results_dir = Path(results_dir).resolve()
        with pytest.raises(SystemExit) as exit:
            main(
                SPECIFICATIONS_DIR / "pipeline.yaml",
                SPECIFICATIONS_DIR / "input_data.yaml",
                SPECIFICATIONS_DIR / "environment_local.yaml",
                results_dir,
            )
        assert exit.value.code == 1
        assert "MissingOutputException" in caplog.text
