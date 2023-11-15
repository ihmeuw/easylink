from pathlib import Path

import linker

BASE_DIR = Path(linker.__file__).resolve().parent
CONTAINER_DIR = BASE_DIR / "containers"
