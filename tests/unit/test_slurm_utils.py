from src.linker.utilities.slurm_utils import _get_cli_args


def test__get_cli_args():
    kwargs = {
        "job_name": "some-job-name",
        "account": "some-account",
        "partition": "some-partition",
        "peak_memory": 42,
        "max_runtime": 24,
        "num_threads": 7,
    }

    cli_args = _get_cli_args(**kwargs)
    assert cli_args == (
        f"-J {kwargs['job_name']} "
        f"-A {kwargs['account']} "
        f"-p {kwargs['partition']} "
        f"--mem={kwargs['peak_memory']*1024} "
        f"-t {kwargs['max_runtime']}:00:00 "
        f"-c {kwargs['num_threads']}"
    )

    # Ensure that there are spaces between each item. When splitting on spaces,
    # we expect there to be 11 items (not 12 b/c "--mem=X" does not count as 2 items)
    assert len(cli_args.split(" ")) == 11
