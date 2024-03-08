from pathlib import Path
from typing import Callable

import pytest

from linker.step import Step


def test_step_instantiation():
    step = Step("foo")
    assert step.name == "foo"
    assert isinstance(step.validate_file, Callable)
