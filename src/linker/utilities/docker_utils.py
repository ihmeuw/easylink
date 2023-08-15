from pathlib import Path

import docker
from loguru import logger


def is_docker_daemon_running():
    try:
        client = docker.from_env()
        client.ping()
        return True
    except:
        return False


def load_docker_image(image_path: Path):
    logger.info(f"Loading the image ({image_path})")
    try:
        client = docker.from_env()
        with open(image_path, "rb") as f:
            image_data = f.read()
        loaded_image = client.images.load(image_data)[0]
        image_id = loaded_image.short_id.split(":")[1]
        logger.info(f"Loaded image with image ID {image_id}")
    except Exception as e:
        raise RuntimeError(f"Error loading image: {e}")
    return image_id


def run_docker_container(image_id: str, input_data_path: Path, results_path: Path):
    logger.info(f"Running the container from image {image_id}")
    client = docker.from_env()
    volumes = {
        str(input_data_path): {"bind": "/app/input_data", "mode": "ro"},
        str(results_path): {"bind": "/app/results", "mode": "rw"},
    }
    container = client.containers.run(
        image_id, volumes=volumes, detach=True, auto_remove=True, tty=True
    )
    logs = container.logs(stream=True, follow=True, stdout=True, stderr=True)
    with open(results_path / "docker.o", "wb") as output_file:
        for log in logs:
            output_file.write(log)
    container.wait()


def remove_docker_image(image_id):
    try:
        client = docker.from_env()
        client.images.remove(image_id)
        logger.info(f"Removed image {image_id}")
    except Exception as e:
        logger.error(f"Error removing image {image_id}: {e}")
