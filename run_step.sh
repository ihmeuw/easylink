#!/bin/bash

set -e

USAGE_COMMENT="
*** ERROR ***
Usage: $0 <STEP-FOLDER> <CONTAINER-PLATFORM>
Arguments:
    STEP-FOLDER - Name of step-specific sub-directory
    CONTAINER-PLATFORM - 'docker' or 'singularity'
"

# TODO: support user-provided input files
# e.g. docker cp some-directory/* $image_id:/app/ 

if [ "$#" -lt 2 ]; then
    echo "$USAGE_COMMENT"
    echo "Please provide all required arguments."
    exit 1
fi

DIR=$(realpath "$1")
PLATFORM="$2"

echo "$DIR"
if [ ! -d "$DIR" ]; then
    echo "$USAGE_COMMENT"
    echo "STEP-FOLDER provided ('$DIR') does not exist."
    exit 1
fi

if [ "$PLATFORM" != "docker" ] && [ "$PLATFORM" != "singularity" ]; then
    echo "$USAGE_COMMENT"
    echo "CONTAINER-PLATFORM  must be 'docker' or 'singularity'; provided '$PLATFORM'."
    exit 1
fi

STEP=$(basename $DIR)

# TODO: implement versioning
mkdir $DIR/results

if [ "$PLATFORM" = "docker" ]; then
    echo ""
    echo "Loading the image"
    # TODO: make flexibile to different compressions (bzip2 or xz) or no compression
    docker load -i $DIR/image.tar.gz 2>&1 | tee $DIR/results/docker.o
    IMAGE_ID=$(docker images --filter=reference=linker:$STEP --format "{{.ID}}")
    echo "Loaded image with image ID $IMAGE_ID"

    echo ""
    echo "Running the image"
    docker run --rm -v $DIR/input_data:/app/input_data/ -v $DIR/results:/app/results/ $IMAGE_ID 2>&1 | tee -a $DIR/results/docker.o

    echo ""
    echo "Removing the image"
    docker rmi $IMAGE_ID
fi

if [ "$PLATFORM" = "singularity" ]; then
    echo ""
    echo "Converting the docker image to singularity"
    singularity build $DIR/image.sif docker-archive://$DIR/image.tar.gz 2>&1 | tee $DIR/results/singularity.o

    echo ""
    echo "Running the image"
    singularity run $DIR/image.sif 2>&1 | tee -a $DIR/results/singularity.o

    echo ""
    echo "Moving image file into results"
    mv $DIR/image.sif $DIR/results
fi

echo ""
echo "*** finished running the step ($PLATFORM) ***"
echo ""
