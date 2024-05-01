from pathlib import Path
from typing import Callable

import pytest
from easylink.step import Step


def test_step_instantiation():
    step = Step("foo")
    assert step.name == "foo"
    assert isinstance(step.input_validator, Callable)
