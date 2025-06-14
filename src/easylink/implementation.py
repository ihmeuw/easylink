"""
===============
Implementations
===============

This module is responsible for defining the abstractions that represent actual
implementations of steps in a pipeline. Typically, these abstractions contain
information about what container to run for a given step and other related details.

"""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING

from layered_config_tree import LayeredConfigTree
from loguru import logger

from easylink.utilities import paths
from easylink.utilities.data_utils import (
    calculate_md5_checksum,
    download_image,
    load_yaml,
)

if TYPE_CHECKING:
    from easylink.graph_components import InputSlot, OutputSlot


class Implementation:
    """A representation of an actual container that will be executed for a :class:`~easylink.step.Step`.

    ``Implementations`` exist at a lower level than ``Steps``. This class contains
    information about what container to use, what environment variables to set
    inside the container, and some metadata about the container.

    Parameters
    ----------
    schema_steps
        The user-requested ``Step`` names for which this ``Implementation`` is
        expected to implement.
    implementation_config
        The configuration details required to run the relevant container.
    input_slots
        All required :class:`InputSlots<easylink.graph_components.InputSlot>`.
    output_slots
        All required :class:`OutputSlots<easylink.graph_components.OutputSlot>`.
    """

    def __init__(
        self,
        schema_steps: list[str],
        implementation_config: LayeredConfigTree,
        input_slots: Iterable[InputSlot] = (),
        output_slots: Iterable[OutputSlot] = (),
        is_auto_parallel: bool = False,
    ):
        self.name = implementation_config.name
        """The name of this ``Implementation``."""
        self.input_slots = {slot.name: slot for slot in input_slots}
        """A mapping of ``InputSlot`` names to their instances."""
        self.output_slots = {slot.name: slot for slot in output_slots}
        """A mapping of ``OutputSlot`` names to their instances."""
        self._metadata = self._load_metadata()
        self.environment_variables = self._get_env_vars(implementation_config)
        """A mapping of environment variables to set."""
        self.metadata_steps = self._metadata["steps"]
        """The names of the specific ``Steps`` for which this ``Implementation`` 
        has been designed to implement."""
        self.schema_steps = schema_steps
        """The names of the specific ``Steps`` that the user has requested to be
        implemented by this particular ``Implementation``."""
        self.requires_spark = self._metadata.get("requires_spark", False)
        """Whether this ``Implementation`` requires a Spark environment."""
        self.is_auto_parallel = is_auto_parallel

    def __repr__(self) -> str:
        return f"Implementation.{self.name}"

    def validate(self, skip_image_validation: bool, images_dir: str | Path) -> list[str]:
        """Validates individual ``Implementation`` instances.

        Returns
        -------
            A list of logs containing any validation errors. Each item in the list
            is a distinct message about a particular validation error (e.g. if a
            required image does not exist).

        Notes
        -----
        This is intended to be run from :meth:`easylink.pipeline.Pipeline._validate`.
        """
        logs = []
        logs = self._validate_expected_steps(logs)
        if not skip_image_validation:
            logs = self._download_and_validate_image(logs, images_dir)
        return logs

    ##################
    # Helper methods #
    ##################

    def _load_metadata(self) -> dict[str, str]:
        """Loads the relevant implementation metadata."""
        metadata = load_yaml(paths.IMPLEMENTATION_METADATA)
        return metadata[self.name]

    def _validate_expected_steps(self, logs: list[str]) -> list[str]:
        """Validates that the ``Implementation`` is responsible for the correct steps."""
        if not set(self.schema_steps) == set(self.metadata_steps):
            logs.append(
                f"Pipeline configuration nodes {self.schema_steps} do not match "
                f"metadata steps {self.metadata_steps}."
            )
        return logs

    def _download_and_validate_image(
        self, logs: list[str], images_dir: str | Path
    ) -> list[str]:
        """Downloads the image if required and validates it exists.

        If the image does not exist in the specified images directory, it will
        attempt to download it.
        """
        # HACK: We manually create the image path here as well as later when writing
        # each implementations Snakefile rule.
        image_path = Path(images_dir) / self.singularity_image_name
        expected_md5_checksum = self._metadata.get("md5_checksum", None)
        record_id = self._metadata.get("zenodo_record_id", None)
        if image_path.exists():
            self._handle_conflicting_checksums(
                logs, image_path, expected_md5_checksum, record_id
            )
        else:
            if not record_id:
                logs.append(
                    f"Image '{str(image_path)}' does not exist and no Zenodo record ID "
                    "is provided to download it."
                )
            if not expected_md5_checksum:
                logs.append(
                    f"Image '{str(image_path)}' does not exist and no MD5 checksum "
                    "is provided to verify from the host."
                )
            if not record_id or not expected_md5_checksum:
                return logs
            download_image(
                images_dir=images_dir,
                record_id=record_id,
                filename=self.singularity_image_name,
                md5_checksum=expected_md5_checksum,
            )
        if not image_path.exists():
            logs.append(
                f"Image '{str(image_path)}' does not exist and could not be downloaded."
            )
        return logs

    @staticmethod
    def _handle_conflicting_checksums(
        logs: list[str],
        image_path: Path,
        expected_md5_checksum: str | None,
        record_id: str | None,
    ) -> list[str]:
        # TODO: Strengthen the following logic to better handle image updates.
        # If using the default images directory and the image already exists
        # but with a different checksum than in the implementation metadata,
        # re-download.
        calculated_md5_checksum = calculate_md5_checksum(image_path)
        if (
            image_path.parent == paths.DEFAULT_IMAGES_DIR
            and expected_md5_checksum
            and calculated_md5_checksum != expected_md5_checksum
        ):
            if not record_id:
                logs.append(
                    f"Image '{str(image_path)}' exists but has a different MD5 checksum "
                    f"({calculated_md5_checksum}) than expected ({expected_md5_checksum}). "
                    "No Zenodo record ID is provided to re-download the image."
                )
            logger.info(
                f"Image '{str(image_path)}' exists but has a different MD5 checksum "
                f"({calculated_md5_checksum}) than expected ({expected_md5_checksum}). "
                "Re-downloading the image."
            )
            download_image(
                images_dir=image_path.parent,
                record_id=record_id,
                filename=image_path.name,
                md5_checksum=expected_md5_checksum,
            )
        return logs

    def _get_env_vars(self, implementation_config: LayeredConfigTree) -> dict[str, str]:
        """Gets the relevant environment variables."""
        env_vars = self._metadata.get("env", {})
        env_vars.update(implementation_config.get("configuration", {}))
        return env_vars

    @property
    def singularity_image_name(self) -> str:
        """The path to the required Singularity image."""
        return self._metadata["image_name"]

    @property
    def script_cmd(self) -> str:
        """The command to run inside of the container."""
        return self._metadata["script_cmd"]

    @property
    def outputs(self) -> dict[str, list[str]]:
        """The expected output paths. If output metadata is provided, use it. Otherwise,
        assume that the output is a sub-directory with the name of the output slot.
        If there is only one output slot, use '.'."""
        if len(self.output_slots) == 1:
            return self._metadata.get("outputs", {list(self.output_slots.keys())[0]: "."})
        return {
            output_slot_name: self._metadata.get("outputs", {}).get(
                output_slot_name, output_slot_name
            )
            for output_slot_name in self.output_slots
        }


class NullImplementation:
    """A partial :class:`Implementation` interface when no container is needed to run.

    The primary use case for this class is to be able to add a :class:`~easylink.step.Step`
    that does *not* have a corresponding ``Implementation`` to an :class:`~easylink.graph_components.ImplementationGraph`
    since adding any new node requires an object with :class:`~easylink.graph_components.InputSlot`
    and :class:`~easylink.graph_components.OutputSlot` names.

    Parameters
    ----------
    name
        The name of this ``NullImplementation``.
    input_slots
        All required ``InputSlots``.
    output_slots
        All required ``OutputSlots``.

    """

    def __init__(
        self,
        name: str,
        input_slots: Iterable[InputSlot] = (),
        output_slots: Iterable[OutputSlot] = (),
    ):
        self.name = name
        """The name of this ``NullImplementation``."""
        self.input_slots = {slot.name: slot for slot in input_slots}
        """A mapping of ``InputSlot`` names to their instances."""
        self.output_slots = {slot.name: slot for slot in output_slots}
        """A mapping of ``OutputSlot`` names to their instances."""
        self.schema_steps = [self.name]
        """The requested :class:`~easylink.step.Step` names this ``NullImplementation`` implements."""
        self.combined_name = None
        """The name of the combined implementation of which ``NullImplementation`` 
        is a constituent. This is definitionally None."""


class NullSplitterImplementation(NullImplementation):
    """A type of :class:`NullImplementation` specifically for :class:`SplitterSteps<easylink.step.SplitterStep>`.

    See ``NullImplementation`` for inherited attributes.

    Parameters
    ----------
    splitter_func_name
        The name of the splitter function to use.

    """

    def __init__(
        self,
        name: str,
        input_slots: Iterable[InputSlot],
        output_slots: Iterable[OutputSlot],
        splitter_func_name: str,
    ):
        super().__init__(name, input_slots, output_slots)
        self.splitter_func_name = splitter_func_name
        """The name of the splitter function to use."""


class NullAggregatorImplementation(NullImplementation):
    """A type of :class:`NullImplementation` specifically for :class:`AggregatorSteps<easylink.step.AggregatorStep>`.

    See ``NullImplementation`` for inherited attributes.

    Parameters
    ----------
    aggregator_func_name
        The name of the aggregation function to use.
    splitter_node_name
        The name of the :class:`~easylink.step.SplitterStep` and its corresponding
        :class:`NullSplitterImplementation` that did the splitting.

    """

    def __init__(
        self,
        name: str,
        input_slots: Iterable[InputSlot],
        output_slots: Iterable[OutputSlot],
        aggregator_func_name: str,
        splitter_node_name: str,
    ):
        super().__init__(name, input_slots, output_slots)
        self.aggregator_func_name = aggregator_func_name
        """The name of the aggregation function to use."""
        self.splitter_node_name = splitter_node_name
        """The name of the :class:`~easylink.step.SplitterStep` and its corresponding
        :class:`NullSplitterImplementation` that did the splitting."""


class PartialImplementation:
    """One part of a combined implementation that spans multiple :class:`Steps<easylink.step.Step>`.

    A ``PartialImplementation`` is what is initially added to the
    :class:`~easylink.graph_components.ImplementationGraph` when a so-called
    "combined implementation" is used (i.e. an :class:`Implementation` that spans
    multiple ``Steps``). We initially add a node for *each* ``Step``, which has as
    its ``implementation`` attribute a ``PartialImplementation``. Such a graph is not
    yet fit to run. When we make our second pass through, after the flat (non-hierarchical)
    :class:`~easylink.pipeline_graph.PipelineGraph` has been created, we find the
    set of ``PartialImplementation`` nodes corresponding to each combined implementation
    and replace them with a single node with a true ``Implementation`` representing
    the combined implementation.

    Parameters
    ----------
    combined_name
        The name of the combined implementation of which this ``PartialImplementation``
        is a part.
    schema_step
        The requested ``Step`` name that this ``PartialImplementation`` partially
        implements.
    input_slots
        The :class:`InputSlots<easylink.graph_components.InputSlot>` for this ``PartialImplementation``.
    output_slots
        The :class:`OutputSlots<easylink.graph_components.OutputSlot>` for this ``PartialImplementation``.

    """

    def __init__(
        self,
        combined_name: str,
        schema_step: str,
        input_slots: Iterable[InputSlot] = (),
        output_slots: Iterable[OutputSlot] = (),
    ):
        self.combined_name = combined_name
        """The name of the combined implementation of which this ``PartialImplementation``
        is a part."""
        self.schema_step = schema_step
        """The requested ``Step`` name that this ``PartialImplementation`` partially 
        implements."""
        self.input_slots = {slot.name: slot for slot in input_slots}
        """A mapping of ``InputSlot`` names to their instances."""
        self.output_slots = {slot.name: slot for slot in output_slots}
        """A mapping of ``OutputSlot`` names to their instances."""
