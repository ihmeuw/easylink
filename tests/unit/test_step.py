from linker.step import Step


def test_step_instantiation():
    step = Step("foo")
    assert step.name == "foo"
