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

from pathlib import Path

from loguru import logger


def main(script_path: Path) -> None:
    """Creates a container to run a specific script and registers it with EasyLink.

    Parameters
    ----------
    script_path
        The filepath to a single script that implements a step of the pipeline.
    """
    creator = ImplementationCreator(script_path)
    creator.create_recipe()
    creator.build_container()
    creator.move_container()
    creator.register()


class ImplementationCreator:
    """A class used to create a container for a specific implementation."""

    def __init__(self, script_path: Path) -> None:
        self.script_path = script_path
        self.implementation_name = script_path.stem
        self.requirements = self._extract_requirements(script_path)
        self.step_name = self._extract_step_name(script_path)

    def create_recipe(self) -> None:
        """Builds the singularity recipe and writes it to disk."""

        recipe = PythonRecipe(self.script_path, self.requirements)
        recipe.build()
        recipe.write()
        pass

    def build_container(self) -> None:
        """Builds the container from the recipe."""
        logger.info(f"Building container for '{self.implementation_name}'")
        pass

    def move_container(self) -> None:
        """Moves the container to the proper location for EasyLink to find it."""
        logger.info(f"Moving container '{self.implementation_name}'")
        pass

    def register(self) -> None:
        """Registers the container with EasyLink.

        Specifically, this function adds the implementation details to the
        implementation_metadata.yaml registry file.
        """
        logger.info(f"Registering container '{self.implementation_name}'")
        pass

    @staticmethod
    def _extract_requirements(script_path: Path) -> str:
        """Extracts the script's dependency requirements.

        The expectation is that the requirements are specified within the script
        as a comment of the format:

        .. code-block:: python
            # REQUIREMENTS: pandas==2.1.2 pyarrow pyyaml

        The requirements must be specified as a single space-separated line.
        """
        requirements = _extract_metadata("REQUIREMENTS", script_path)
        if len(requirements) == 0:
            logger.info(f"No requirements found in {script_path}.")
            requirements.append("")
        return requirements[0]

    @staticmethod
    def _extract_step_name(script_path: Path) -> str:
        """Extracts the name of the step that this script is implementing.

        The expectation is that the step's name is specified within the script
        as a comment of the format:

        .. code-block:: python
            # STEP_NAME: blocking
        """
        step_name = _extract_metadata("STEP_NAME", script_path)
        if len(step_name) == 0:
            raise ValueError(
                f"Could not find a step name in {script_path}. "
                "Please ensure the script contains a comment of the form '# STEP_NAME: <name>'"
            )
        return step_name[0]


class PythonRecipe:
    """A singularity recipe generator specific to implementations written in Python."""

    BASE_IMAGE = (
        "python@sha256:1c26c25390307b64e8ff73e7edf34b4fbeac59d41da41c08da28dc316a721899"
    )

    def __init__(self, script_path: Path, requirements: str) -> None:
        self.script_path = script_path
        self.requirements = requirements
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
    python /{script_name} '$@'"""

    def write(self) -> None:
        """Writes the recipe to disk."""
        logger.info(f"Writing recipe for '{self.script_path.stem}' to disk.")
        recipe_path = self.script_path.with_suffix(".def")
        if not self.text:
            raise ValueError("No recipe text to build.")
        if recipe_path.exists():
            logger.warning(f"Recipe file {recipe_path} already exists. Overwriting it.")
        recipe_path.write_text(self.text)
        if not recipe_path.exists():
            raise FileNotFoundError(f"Failed to write recipe to {recipe_path}.")


####################
# Helper functions #
####################


def _extract_metadata(key: str, script_path: Path) -> list[str]:
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
