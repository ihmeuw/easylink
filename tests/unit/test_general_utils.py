# mypy: ignore-errors
import errno

import pytest

from easylink.utilities.general_utils import exit_with_validation_error


def test_exit_with_validation_error():
    with pytest.raises(SystemExit) as e:
        exit_with_validation_error({"error": "error message"})
    assert e.value.code == errno.EINVAL
