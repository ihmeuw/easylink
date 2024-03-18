from pathlib import Path

# TODO: We'll need to update this to be more generic for external users and have a way of configuring this
CONTAINER_DIR = "/mnt/team/simulation_science/priv/engineering/er_ecosystem/images"
IMPLEMENTATION_METADATA = Path(__file__).parent.parent / "implementation_metadata.yaml"
LINKER_TEMP = {"local": Path("/tmp/linker"), "slurm": Path("/tmp")}
