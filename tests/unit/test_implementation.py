from pathlib import Path

import pytest

from linker.implementation import Implementation
from linker.step import Step


@pytest.mark.skip("integration test blah blah walla walla")
def test_fails_when_missing_results(default_config, mocker):
    pass
