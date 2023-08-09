#!/bin/bash

set -e

usage_comment="Usage: $0 <step path>";

# TODO: support a "singularity" or "docker" input arg
# TODO: support user-provided input files
# e.g. docker cp some-directory/* $image_id:/app/ 

if [[ -z "$1" ]]; then
    echo $usage_comment
    echo "Please provide the step-specific directory."
    exit 1
fi

dir=$(realpath "$1");
step=$(basename $dir);

# TODO: implement versioning
mkdir $dir/results

echo ""
echo "Loading the image"
# TODO: make flexibile to different compressions (bzip2 or xz) or no compression
docker load -i $dir/image.tar.gz 2>&1 | tee $dir/results/docker.o
image_id=$(docker images --filter=reference=linker:$step --format "{{.ID}}")
echo "Loaded image with image ID $image_id"

echo ""
echo "Running the step"
docker run --rm -v $dir/input_data:/app/input_data/ -v $dir/results:/app/results/ $image_id 2>&1 | tee -a $dir/results/docker.o

echo ""
echo "Removing image"
docker rmi $image_id

echo ""
echo "*** finished ***"
echo ""
