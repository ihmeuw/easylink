import subprocess
import os
import shutil

# This script just tests that the dummy containers can actually be iterated (each taking as input the
# output of the previous). It is not part of the dummy containers themselves!


# https://stackoverflow.com/a/185941/
def rm_in_directory(directory):
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print("Failed to delete %s. Reason: %s" % (file_path, e))


user = os.environ["USER"]
singularity_tmp = f"/tmp/singularity_{user}/tmp"
singularity_workdir = f"/tmp/singularity_{user}/workdir"
results_dir = f"/tmp/singularity_{user}/results"
subprocess.run(["mkdir", "-p", singularity_tmp])
subprocess.run(["mkdir", "-p", singularity_workdir])
subprocess.run(["mkdir", "-p", results_dir])
rm_in_directory(singularity_tmp)
rm_in_directory(singularity_workdir)
rm_in_directory(results_dir)

input_file = os.getenv("DUMMY_CONTAINERS_TEST_INPUT_FILE", "./input_file_2.csv")
input_files = [input_file]
input_file_format = input_file.split(".")[-1]

container_engine = os.getenv("DUMMY_CONTAINERS_TEST_CONTAINER_ENGINE", "singularity")

# Useful for debugging: you don't need to rebuild the containers for script changes if you use this flag.
use_latest_scripts = os.getenv(
    "DUMMY_CONTAINERS_TEST_USE_LATEST_SCRIPTS", "false"
).lower() in ("true", "1", "t")

num_steps = 30

import random

random.seed(1234)

implementations = ["python_pandas", "r", "python_pyspark"]

steps = [
    {
        "implementation": random.choice(implementations),
        "output_file_format": random.choice(["csv", "parquet"]),
    }
    for _ in range(num_steps)
]

if container_engine == "docker" and os.getenv(
    "DUMMY_CONTAINERS_TEST_FROM_TAR", "false"
).lower() in ("true", "1", "t"):
    for implementation in implementations:
        command = [
            "docker",
            "load",
            "-i",
            f'./{implementation}/{implementation.replace("_", "-")}-image.tar.gz',
        ]
        print(" ".join(command))
        subprocess.check_output(command)

files_so_far = input_files.copy()

for i, step in enumerate(steps):
    print(step)

    step_results_dir = f"{results_dir}/step_{i}"
    subprocess.run(["mkdir", "-p", step_results_dir])

    implementation = step["implementation"]
    output_file_format = step["output_file_format"]

    if (
        os.getenv("DUMMY_CONTAINERS_TEST_INCLUDE_EXTRA_OUTPUTS", "false").lower()
        in ("true", "1", "t")
        and random.random() < 0.25
    ):
        output_files = [
            f"{step_results_dir}/result_1.{output_file_format}",
            f"{step_results_dir}/result_2.{output_file_format}",
        ]
    else:
        output_files = [f"{step_results_dir}/result.{output_file_format}"]

    env_var_args = {
        "OUTPUT_FILE_FORMAT": output_file_format,
        "OUTPUT_PATHS": ",".join(
            [
                output_file.replace(step_results_dir, "/results")
                for output_file in output_files
            ]
        ),
    }

    if (
        os.getenv("DUMMY_CONTAINERS_TEST_INCLUDE_BROKEN", "false").lower()
        in ("true", "1", "t")
        and random.random() < 0.25
    ):
        env_var_args["BROKEN"] = "yes"

    if os.getenv("DUMMY_CONTAINERS_TEST_INCLUDE_INCREMENT", "false").lower() in (
        "true",
        "1",
        "t",
    ):
        env_var_args["INCREMENT"] = int(random.random() * 3) + 1

    bindings = [
        (step_results_dir, "/results"),
        (singularity_tmp, "/tmp"),
    ]
    if use_latest_scripts:
        if implementation == "python_pyspark":
            bindings.append(("./python_pyspark/dummy_step.py", "/code/dummy_step.py"))
        elif implementation == "python_pandas":
            bindings.append(("./python_pandas/dummy_step.py", "/dummy_step.py"))
        elif implementation == "r":
            bindings.append(("./r/dummy_step.R", "/dummy_step.R"))
        else:
            raise ValueError()

    for index, input_file in enumerate(input_files):
        input_path_inside_container = (
            f"/input_data/main_input_file_{index}.{input_file_format}"
        )
        bindings.append((input_file, input_path_inside_container))

    if (
        os.getenv("DUMMY_CONTAINERS_TEST_INCLUDE_EXTRA_INPUTS", "false").lower()
        in ("true", "1", "t")
        and random.random() < 0.1
    ):
        second_main_input_file = random.choice(files_so_far)
        second_main_input_file_format = second_main_input_file.split(".")[-1]
        path_inside_container = (
            f"/input_data/main_input_file_2.{second_main_input_file_format}"
        )
        bindings.append((second_main_input_file, path_inside_container))

    if (
        os.getenv("DUMMY_CONTAINERS_TEST_INCLUDE_EXTRA_INPUTS", "false").lower()
        in ("true", "1", "t")
        and random.random() < 0.1
    ):
        secondary_input_file = random.choice(files_so_far)
        secondary_input_file_format = secondary_input_file.split(".")[-1]
        path_inside_container = f"/input_data/secondary_input.{secondary_input_file_format}"
        env_var_args["SECONDARY_INPUT_FILE_PATHS"] = path_inside_container
        bindings.append((secondary_input_file, path_inside_container))

    if (
        os.getenv("DUMMY_CONTAINERS_TEST_INCLUDE_EXTRA_INPUTS", "false").lower()
        in ("true", "1", "t")
        and random.random() < 0.1
    ):
        implementation_specific_input_file = random.choice(files_so_far)
        implementation_specific_input_file_format = implementation_specific_input_file.split(
            "."
        )[-1]
        path_inside_container = f"/extra_implementation_specific_input_data/input.{implementation_specific_input_file_format}"
        env_var_args["EXTRA_IMPLEMENTATION_SPECIFIC_INPUT_FILE_PATH"] = path_inside_container
        bindings.append((implementation_specific_input_file, path_inside_container))

    if os.getenv("DUMMY_CONTAINERS_TEST_INCLUDE_EXTRA_OUTPUTS", "false").lower() in (
        "true",
        "1",
        "t",
    ):
        bindings.append((step_results_dir, "/diagnostics/"))

    env_var_args = {f"DUMMY_CONTAINER_{k}": v for k, v in env_var_args.items()}

    if implementation == "python_pyspark":
        bindings.append((singularity_workdir, "/workdir"))
        workdir = "/workdir"
    else:
        workdir = "/"

    env = {**os.environ}

    if container_engine == "singularity":
        image = f'./{implementation}/{implementation.replace("_", "-")}-image.sif'
        command = [
            "singularity",
            "run",
            "--pwd",
            workdir,
            "-B",
            ",".join([f"{k}:{v}" for k, v in bindings]),
            image,
        ]
        env = {**env, **{f"SINGULARITYENV_{k}": v for k, v in env_var_args.items()}}
    elif container_engine == "docker":
        image = f"linker:dummy_container_{implementation}"
        command = [
            "docker",
            "run",
        ]
        for k, v in bindings:
            command += ["--mount", f"type=bind,source={k},target={v}"]

        for k, v in env_var_args.items():
            command += ["-e", f"{k}={v}"]

        command += ["-i", "-t", image]
    else:
        raise ValueError()

    print(" ".join(command))
    print(env_var_args)
    subprocess.check_output(command, env=env)

    files_so_far += output_files
    input_files = output_files
    input_file_format = output_file_format

    # Just to ensure that steps aren't communicating in a way they shouldn't
    rm_in_directory(singularity_tmp)
    rm_in_directory(singularity_workdir)
