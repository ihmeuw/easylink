from pathlib import Path
from typing import Callable

import pytest

from linker.step import Step


def test_step_instantiation():
    step = Step("foo")
    assert step.name == "foo"
    assert isinstance(step.validate_file, Callable)


def test_fails_when_no_results_made():
    """We test against a step's output validation in test_validation.py. This test
    ensures that expected runtime error happens if a step does NOT produce any results.
    """
    pass
