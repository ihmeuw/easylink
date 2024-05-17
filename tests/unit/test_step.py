from pathlib import Path
from typing import Callable

import pytest

from easylink.step import Step


def test_step_instantiation():
    step = Step("foo", prev_input=True, input_files=True)
    assert step.name == "foo"
    assert step.prev_input
    assert step.input_files
    assert isinstance(step.input_validator, Callable)
