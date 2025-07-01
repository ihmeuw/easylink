"""

This module contains commonly-used filepaths and directories.

"""

from pathlib import Path

DEV_IMAGES_DIR = "/mnt/team/simulation_science/priv/engineering/er_ecosystem/images"
"""Path to the directory where the development/dummy pipeline images are stored."""
DEFAULT_IMAGES_DIR = Path.home() / ".easylink_images"
"""Default subdirectory for storing downloaded images."""
IMPLEMENTATION_METADATA = Path(__file__).parent.parent / "implementation_metadata.yaml"
"""Path to the implementation metadata file."""
EASYLINK_TEMP = {"local": Path("/tmp/easylink"), "slurm": Path("/tmp")}
"""Paths to the easylink tmp/ directory to get bound to the container's /tmp directory.
When running on slurm, we bind /tmp (rather than /tmp/easylink) to avoid creating 
a subdir with a prolog script"""
SPARK_SNAKEFILE = Path(__file__).parent / "spark.smk"
"""Path to the Snakemake snakefile containing spark-specific rules."""
