"""
An EasyLink "implementation" (related to, but not to be confused with the
:class:`~easylink.implementation.Implementation` class object) is, at the most
basic level, a container that implements some step of the pipeline as well as
other supporting information to connect said container to the EasyLink framework.

In order to create an implementation, three things are needed:
1. The container that runs the script that implements a step of the pipeline must be created.
2. The container must be moved to the proper hosting location so EasyLink can find it.
3. The container must be registered with EasyLink so it can be used.

"""

import shutil
import subprocess
from pathlib import Path
from typing import cast

import yaml
from loguru import logger

from easylink.pipeline_schema_constants import SCHEMA_PARAMS
from easylink.step import (
    AutoParallelStep,
    ChoiceStep,
    HierarchicalStep,
    IOStep,
    Step,
    TemplatedStep,
)
from easylink.utilities.data_utils import load_yaml
from easylink.utilities.paths import DEV_IMAGES_DIR, IMPLEMENTATION_METADATA


def main(script_path: Path, host: Path) -> None:
    """Creates a container to run a specific script and registers it with EasyLink.

    Parameters
    ----------
    script_path
        The filepath to a single script that implements a step of the pipeline.
    host
        The host directory to move the container to.
    """
    creator = ImplementationCreator(script_path, host)
    creator.create_recipe()
    creator.build_container()
    creator.move_container()
    creator.register()


class ImplementationCreator:
    """A class used to create a container for a specific implementation.

    Parameters
    ----------
    script_path
        The filepath to a single script that implements a step of the pipeline.
    host
        The host directory to move the container to.
    recipe_path
        The filepath to the recipe file that will be created. It will be created
        in the same directory as the script.
    local_container_path
        The filepath to the local container that will be created. It will be created
        in the same directory as the script.
    hosted_container_path
        The filepath to to move the container to. This is where EasyLink will look
        for the container.
    implementation_name
        The name of the implementation. It is by definition the name of the script.
    step
        The name of the step that this implementation implements.
    output_slot
        The name of the output slot that this implementation sends results to.
    """

    def __init__(self, script_path: Path, host: Path) -> None:
        self.script_path = script_path
        """The filepath to a single script that implements a step of the pipeline."""
        self.host = host
        """The host directory to move the container to."""
        self.recipe_path = script_path.with_suffix(".def")
        """The filepath to the recipe file that will be created. It will be created
        in the same directory as the script."""
        self.local_container_path = script_path.with_suffix(".sif")
        """The filepath to the local container that will be created. It will be created
        in the same directory as the script."""
        self.hosted_container_path = self.host / self.local_container_path.name
        """The filepath to to move the container to. This is where EasyLink will look
        for the container."""
        self.implementation_name = script_path.stem
        """The name of the implementation. It is by definition the name of the script."""
        self.step = self._extract_implemented_step(script_path)
        """The name of the step that this implementation implements."""
        self.has_custom_recipe = self._extract_has_custom_recipe(script_path)
        """Whether the user has already written the recipe for this implementation."""
        self.script_base_command = self._extract_script_base_command(script_path)
        """The base command to use to run the script in this implementation."""
        self.output_slot = self._extract_output_slot(script_path, self.step)
        """The name of the output slot that this implementation sends results to."""

    def create_recipe(self) -> None:
        """Builds the singularity recipe and writes it to disk."""
        if self.has_custom_recipe:
            if not self.recipe_path.exists():
                raise ValueError(f"Could not find a custom recipe at {self.recipe_path}.")
            return

        recipe = PythonRecipe(
            self.script_path,
            self.recipe_path,
            ImplementationCreator._extract_requirements(self.script_path),
            self.script_base_command,
        )
        recipe.build()
        recipe.write()

    def build_container(self) -> None:
        """Builds the container from the recipe.

        Raises
        ------
        subprocess.CalledProcessError
            If the subprocess fails.
        Exception
            If the container fails to build for any reason.
        """
        logger.info(f"Building container for '{self.implementation_name}'")
        if self.local_container_path.exists():
            logger.warning(
                f"Container {self.local_container_path} already exists. Overwriting it."
            )

        try:
            cmd = [
                "singularity",
                "build",
                "--remote",
                "--force",
                str(self.local_container_path),
                str(self.recipe_path),
            ]
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=self.local_container_path.parent,
            )

            # stream output to console
            for line in process.stdout:  # type: ignore[union-attr]
                print(line, end="")
            process.wait()

            if process.returncode == 0:
                logger.info(
                    f"Successfully built container '{self.local_container_path.name}'"
                )
            else:
                logger.error(
                    f"Failed to build container '{self.local_container_path.name}'. "
                    f"Error: {process.returncode}"
                )
                raise subprocess.CalledProcessError(
                    process.returncode, cmd, output=process.stderr.read()  # type: ignore[union-attr]
                )
        except Exception as e:
            logger.error(
                f"Failed to build container '{self.local_container_path.name}'. "
                f"Error: {e}"
            )
            raise

    def move_container(self) -> None:
        """Moves the container to the proper location for EasyLink to find it."""
        logger.info(f"Moving container '{self.implementation_name}' to {self.host}")
        if self.hosted_container_path.exists():
            logger.warning(
                f"Container {self.hosted_container_path} already exists. Overwriting it."
            )
        shutil.move(str(self.local_container_path), str(self.hosted_container_path))

    def register(self) -> None:
        """Registers the container with EasyLink.

        Specifically, this function adds the implementation details to the
        implementation_metadata.yaml registry file.
        """
        logger.info(f"Registering container '{self.implementation_name}'")
        info = load_yaml(IMPLEMENTATION_METADATA)
        if self.implementation_name in info:
            logger.warning(
                f"Implementation '{self.implementation_name}' already exists in the registry. "
                "Overwriting it with the latest data."
            )

        # Handle the fact that developers might be saving to username subdirs
        # If the host folder is a subdirectory of DEV_IMAGES_DIR (e.g., the default
        # host directory when calling `easylink devtools create-implementation`
        # is DEV_IMAGES_DIR/<username>), we want to include the relative path
        # to the DEV_IMAGES_DIR in the image name. This is required because ultimately
        # when running a pipeline, all images are expected to be in a single directory.
        image_name = (
            self.hosted_container_path.name
            # Use just the image name if the hosted path is not a part of DEV_IMAGES_DIR
            if not self.hosted_container_path.is_relative_to(DEV_IMAGES_DIR)
            # Use the path relative to DEV_IMAGES_DIR as the image name
            else str(self.hosted_container_path.relative_to(DEV_IMAGES_DIR))
        )

        info[self.implementation_name] = {
            "steps": [self.step],
            "image_name": str(image_name),
            "script_cmd": f"{self.script_base_command} /{self.script_path.name}",
            "outputs": {
                self.output_slot: "result.parquet",
            },
        }
        self._write_metadata(info)

    @staticmethod
    def _extract_requirements(script_path: Path) -> str:
        """Extracts the script's dependency requirements (if any).

        The expectation is that any requirements are specified within the script
        as a comment of the format:

        .. code-block:: python
            # REQUIREMENTS: pandas==2.1.2 pyarrow pyyaml

        This is an optional field and only required if the script actually has dependencies.

        The requirements must be specified as a single space-separated line.
        """
        requirements = _extract_metadata("REQUIREMENTS", script_path)
        if len(requirements) == 0:
            logger.info(f"No requirements found in {script_path}.")
            requirements.append("")
        return requirements[0]

    @staticmethod
    def _extract_implemented_step(script_path: Path) -> str:
        """Extracts the name of the step that this script is implementing.

        The expectation is that the step's name is specified within the script
        as a comment of the format:

        .. code-block:: python
            # STEP_NAME: blocking
        """
        step_info = _extract_metadata("STEP_NAME", script_path)
        if len(step_info) == 0:
            raise ValueError(
                f"Could not find a step name in {script_path}. "
                "Please ensure the script contains a comment of the form '# STEP_NAME: <name>'"
            )
        steps = [step.strip() for step in step_info[0].split(",")]
        if len(steps) > 1:
            raise NotImplementedError(
                f"Multiple steps are not yet supported. {script_path} is requesting "
                f"to implement {steps}."
            )
        return steps[0]

    @staticmethod
    def _extract_has_custom_recipe(script_path: Path) -> bool:
        """Extracts whether the user has already written the recipe for this implementation.

        The expectation is that this flag is specified within the script
        as a comment of the format:

        .. code-block:: python
            # HAS_CUSTOM_RECIPE: true
        """
        has_custom_recipe = _extract_metadata("HAS_CUSTOM_RECIPE", script_path)
        if len(has_custom_recipe) == 0:
            return False
        else:
            return str(has_custom_recipe[0]).strip().lower() in ["true", "yes"]

    @staticmethod
    def _extract_output_slot(script_path: Path, step_name: str) -> str:
        """Extracts the name of the output slot that this script is implementing."""
        schema_name = ImplementationCreator._extract_pipeline_schema_name(script_path)
        implementable_steps = ImplementationCreator._extract_implementable_steps(schema_name)
        step_names = [step.name for step in implementable_steps]
        if step_name not in step_names:
            raise ValueError(
                f"'{step_name}' does not exist as an implementable step in the '{schema_name}' pipeline schema. "
            )
        duplicates = list(set([step for step in step_names if step_names.count(step) > 1]))
        if duplicates:
            raise ValueError(
                f"Multiple implementable steps with the same name found in the '{schema_name}' "
                f"pipeline schema: {duplicates}."
            )
        implemented_step = [step for step in implementable_steps if step.name == step_name][0]
        if len(implemented_step.output_slots) != 1:
            raise NotImplementedError(
                f"Multiple output slots are not yet supported. {script_path} is requesting "
                f"to implement {step_name} with {len(implemented_step.output_slots)} output slots."
            )
        return list(implemented_step.output_slots)[0]

    @staticmethod
    def _extract_implementable_steps(schema_name: str) -> list[Step]:
        """Extracts all implementable steps from the pipeline schema.

        This method recursively traverses the pipeline schema specified in the script
        to dynamically build a list of all implementable steps.
        """

        def _process_step(node: Step) -> None:
            """Adds `step` to the `implementable_steps` list if it is implementable."""
            if isinstance(node, IOStep):
                return
            elif isinstance(node, TemplatedStep):
                _process_step(node.template_step)
                return
            elif isinstance(node, AutoParallelStep):
                _process_step(node.step)
                return
            elif isinstance(node, ChoiceStep):
                for choice_step in node.choices.values():
                    _process_step(cast(Step, choice_step["step"]))
                return
            elif isinstance(node, HierarchicalStep):
                implementable_steps.append(node)
                for sub_step in node.nodes:
                    _process_step(sub_step)
                return
            else:  # base Step
                implementable_steps.append(node)
                return

        schema_steps, _edges = SCHEMA_PARAMS[schema_name]
        implementable_steps: list[Step] = []
        for schema_step in schema_steps:
            _process_step(schema_step)

        return implementable_steps

    @staticmethod
    def _extract_pipeline_schema_name(script_path: Path) -> str:
        """Extracts the relevant pipeline schema name.

        The expectation is that the pipeline schema's name is specified within the script
        as a comment of the format:

        .. code-block:: python
            # PIPELINE_SCHEMA: development

        If no pipeline schema is specified, "main" will be used by default.
        """
        schema_name_list: list[str] = _extract_metadata("PIPELINE_SCHEMA", script_path)
        schema_name = "main" if len(schema_name_list) == 0 else schema_name_list[0]
        if schema_name not in SCHEMA_PARAMS:
            raise ValueError(f"Pipeline schema '{schema_name}' is not supported.")
        return schema_name

    @staticmethod
    def _extract_script_base_command(script_path: Path) -> str:
        """Extracts the base command to be used to run the script.

        The expectation is that the base command is specified within the script
        as a comment of the format:

        .. code-block:: python
            # SCRIPT_BASE_COMMAND: python

        If no pipeline schema is specified, "python" will be used by default.
        """
        base_command_list: list[str] = _extract_metadata("SCRIPT_BASE_COMMAND", script_path)
        base_command = base_command_list[0] if base_command_list else "python"
        return base_command

    @staticmethod
    def _write_metadata(info: dict[str, dict[str, str]]) -> None:
        """Writes the implementation metadata to disk.

        Parameters
        ----------
        info
            The implementation metadata to write to disk.
        """
        with open(IMPLEMENTATION_METADATA, "w") as f:
            yaml.dump(info, f, sort_keys=False)


class PythonRecipe:
    """A singularity recipe generator specific to implementations written in Python."""

    BASE_IMAGE = (
        "python@sha256:1c26c25390307b64e8ff73e7edf34b4fbeac59d41da41c08da28dc316a721899"
    )

    def __init__(
        self,
        script_path: Path,
        recipe_path: Path,
        requirements: str,
        script_base_command: str,
    ) -> None:
        self.script_path = script_path
        self.recipe_path = recipe_path
        self.requirements = requirements
        self.script_base_command = script_base_command
        self.text: str | None = None

    def build(self) -> None:
        """Builds the recipe for the container."""
        logger.info(f"Building recipe for '{self.script_path.stem}'")

        script_name = self.script_path.name
        self.text = f"""
Bootstrap: docker
From: {self.BASE_IMAGE}

%files
    ./{script_name} /{script_name}

%post
    # Create directories
    mkdir -p /input_data
    mkdir -p /extra_implementation_specific_input_data
    mkdir -p /results
    mkdir -p /diagnostics

    # Install Python packages with specific versions
    pip install {self.requirements}

%environment
    export LC_ALL=C

%runscript
    {self.script_base_command} /{script_name} '$@'"""

    def write(self) -> None:
        """Writes the recipe to disk.

        Raises
        ------
        ValueError
            If there is no recipe text to write a recipe from.
        FileNotFoundError
            If the recipe file was not written to disk.
        """
        logger.info(f"Writing recipe for '{self.script_path.stem}' to disk.")
        if not self.text:
            raise ValueError("No recipe text to build.")
        if self.recipe_path.exists():
            logger.warning(f"Recipe file {self.recipe_path} already exists. Overwriting it.")
        with open(self.recipe_path, "w") as f:
            f.write(self.text)
            f.flush()
        if not self.recipe_path.exists():
            raise FileNotFoundError(f"Failed to write recipe to {self.recipe_path}.")


####################
# Helper functions #
####################


def _extract_metadata(key: str, script_path: Path) -> list[str]:
    """Extracts the container metadata from the script comments.

    Parameters
    ----------
    key
        The key to search for in the script comments, e.g. "REQUIREMENTS" or "STEP_NAME".
    script_path
        The path to the script file.

    Returns
    -------
        A list of metadata values found in the script comments.

    Raises
    ------
    ValueError
        If a key is found multiple times in the script.
    """
    metadata = []
    for line in script_path.read_text().splitlines():
        if key in line:
            packed_line = line.replace(" ", "")
            if packed_line.startswith(f"#{key}:"):
                info = line.split(":")[1].strip()
                metadata.append(info)

    if len(metadata) > 1:
        raise ValueError(
            f"Found multiple {key.lower()} requests in {script_path}: {metadata}"
            f"Please ensure the script contains only one comment of the form '# {key}: <request>'"
        )
    return metadata
