# mypy: ignore-errors
import pytest


@pytest.mark.skip(
    reason="TODO: MIC-4914: Test the cli tool for `easylink run` to ensure UI doesn't change unexpectedly."
)
def test_easylink_cli():
    pass
