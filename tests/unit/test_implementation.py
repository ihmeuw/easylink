from pathlib import Path

import pytest

from linker.implementation import Implementation
from linker.step import Step


# TODO MIC-4921
@pytest.mark.skip("Replace this with an integration test when appropriate")
def test_fails_when_missing_results(default_config, mocker):
    pass
