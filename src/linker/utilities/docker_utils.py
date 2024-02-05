import json
from os import strerror
from pathlib import Path
from time import strftime
from typing import Any, Dict, List, Optional

import docker
from docker import DockerClient
from docker.models.containers import Container
from loguru import logger

DOCKER_TIMEOUT = 360  # seconds


def run_with_docker(
    input_bindings: List[str],
    input_env_vars: Dict[str, str],
    results_dir: Path,
    diagnostics_dir: Path,
    step_id: str,
    container_path: Path,
    config: Optional[Dict[str, str]],
) -> None:
    logger.info(f"Running step {step_id} container with docker")
    client = get_docker_client()
    image_id = _load_image(client, container_path)
    container = _run_container(
        client, image_id, input_bindings, input_env_vars, results_dir, diagnostics_dir, config
    )
    _clean(client, image_id, container)


def get_docker_client() -> DockerClient:
    try:
        client = docker.from_env(timeout=DOCKER_TIMEOUT)
        client.ping()
        return client
    except Exception as e:
        raise EnvironmentError(
            "The Docker daemon is not running; please start Docker."
        ) from e


def _load_image(client: DockerClient, image_path: Path) -> str:
    logger.info(f"Loading the image ({str(image_path)})")
    try:
        with open(str(image_path), "rb") as f:
            image_data = f.read()
        loaded_image = client.images.load(image_data)[0]
        image_id = loaded_image.short_id.split(":")[1]
        logger.info(f"Loaded image with image ID {image_id}")
    except Exception as e:
        raise RuntimeError(f"Error loading image: {e}")
    return image_id


def _run_container(
    client: DockerClient,
    image_id: str,
    input_bindings: Dict[str, str],
    input_env_vars: Dict[str, List[str]],
    results_dir: Path,
    diagnostics_dir: Path,
    config: Optional[Dict[str, str]],
):
    logger.info(f"Running the container from image {image_id}")
    volumes = {}
    for outside_path, inside_path in input_bindings.items():
        volumes[outside_path] = {"bind": f"{inside_path}", "mode": "ro"}
    volumes.update(
        **{
            str(results_dir): {"bind": "/results", "mode": "rw"},
            str(diagnostics_dir): {"bind": "/diagnostics", "mode": "rw"},
        }
    )
    input_env_vars = {
        env_var: json.dumps(container_paths)
        for env_var, container_paths in input_env_vars.items()
    }
    environment = {**config, **input_env_vars}

    try:
        container = client.containers.run(
            image_id,
            volumes=volumes,
            detach=True,
            auto_remove=True,
            tty=True,
            environment=environment,
        )
        logs = container.logs(stream=True, follow=True, stdout=True, stderr=True)
        with open(diagnostics_dir / f"docker.o", "wb") as output_file:
            for log in logs:
                output_file.write(log)
        container.wait()
    except KeyboardInterrupt:
        _clean(client, image_id, container)
        raise
    except Exception as e:
        _clean(client, image_id, container)
        raise RuntimeError(
            f"An error occurred running docker container {container.short_id}: {e}"
        )
    return container


def _clean(client: DockerClient, image_id: str, container: Container) -> None:
    try:
        client.containers.get(container.id)
        running = True
    except docker.errors.NotFound:
        running = False
    if running:
        try:
            container.kill()
            logger.info(f"Removed container {container.short_id}")
        except Exception as e:
            logger.error(f"Error removing container {container.short_id}: {e}")
    try:
        client.images.remove(image_id, force=True)
        logger.info(f"Removed image {image_id}")
    except Exception as e:
        logger.error(f"Error removing image {image_id}: {e}")
