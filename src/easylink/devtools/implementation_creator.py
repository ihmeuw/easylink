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
    """ Creates a container to run a specific script and registers it with EasyLink.

    Parameters
    ----------
    script_path
        The filepath to a single script that implements a step of the pipeline.
    """
    creator = ImplementationCreator(script_path)
    creator.write_recipe()
    creator.build_container()
    creator.move_container()
    creator.register()


class ImplementationCreator:
    """A class used to create a container for a specific implementation.

    """

    def __init__(self, script_path: Path) -> None:
        self.script_path = script_path
        self.implementation_name = script_path.name
        self.requirements = self._extract_requirements(script_path)
        self.step_name = self._extract_step_name(script_path)

    def write_recipe(self) -> None:
        """Builds the singularity recipe and writes it to disk."""

        recipe = PythonRecipe(self.script_path)
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
        ...

    @staticmethod
    def _extract_step_name(script_path: Path) -> str:
        """Extracts the name of the step that this script is implementing.

        The expectation is that the step's name is specified within the script
        as a comment of the format:

        .. code-block:: python
            # STEP_NAME: blocking
        """
        ...


class PythonRecipe:
    """A singularity recipe generator specific to implementations written in Python."""

    def __init__(self, script_path: Path) -> None:
        self.script_path = script_path
        self.implementation_name = script_path.name

    def build(self) -> None:
        """ Builds the recipe for the container."""
        logger.info(f"Building recipe for '{self.implementation_name}'")
        ...

    def write(self) -> None:
        """Writes the recipe to disk."""
        logger.info(f"Writing recipe for '{self.implementation_name}' to disk.")
        ...
        


