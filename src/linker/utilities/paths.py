from pathlib import Path

# TODO: We'll need to update this to be more generic for external users and have a way of configuring this
CONTAINER_DIR = "/mnt/team/simulation_science/priv/engineering/er_ecosystem/images"
IMPLEMENTATION_METADATA = Path(__file__).parent.parent / "implementation_metadata.yaml"
# Bind EasyLink temp dir to /tmp in the container.
# For now, put slurm in /tmp to avoid creating a subdir with a prolog script
EASYLINK_TEMP = {"local": Path("/tmp/easylink"), "slurm": Path("/tmp")}

SPARK_SNAKEFILE = Path(__file__).parent / "spark.smk"
