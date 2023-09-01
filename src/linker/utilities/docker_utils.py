from pathlib import Path

import docker
from docker import DockerClient
from docker.models.containers import Container
from loguru import logger


def run_with_docker(results_dir: Path, step_dir: Path) -> None:
    logger.info("Trying to run container with docker")
    _confirm_docker_daemon_running()
    client = docker.from_env()
    image_id = _load_image(client, step_dir / "image.tar.gz")
    container = _run_container(client, image_id, step_dir / "input_data", results_dir)
    _remove_image(client, image_id, container)


def _confirm_docker_daemon_running() -> None:
    try:
        client = docker.from_env()
        client.ping()
        return
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
    client: DockerClient, image_id: str, input_data_path: Path, results_dir: Path
):
    logger.info(f"Running the container from image {image_id}")
    volumes = {
        str(input_data_path): {"bind": "/app/input_data", "mode": "ro"},
        str(results_dir): {"bind": "/app/results", "mode": "rw"},
    }
    try:
        container = client.containers.run(
            image_id, volumes=volumes, detach=True, auto_remove=True, tty=True
        )
        logs = container.logs(stream=True, follow=True, stdout=True, stderr=True)
        with open(results_dir / "docker.o", "wb") as output_file:
            for log in logs:
                output_file.write(log)
        # container.wait()
    except KeyboardInterrupt:
        _remove_image(client, image_id, container)
        raise
    except Exception as e:
        _remove_image(client, image_id, container)
        raise RuntimeError(
            f"An error occurred running docker container {container.short_id}: {e}"
        )
    return container


def _remove_image(client: DockerClient, image_id: str, container: Container) -> None:
    container.wait()
    try:
        client.images.remove(image_id, force=True)
        logger.info(f"Removed image {image_id}")
    except Exception as e:
        logger.error(f"Error removing image {image_id}: {e}")
