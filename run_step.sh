#!/bin/bash

usage_comment="Usage: $0 <step path>";

if [[ -z "$1" ]]; then
    echo $usage_comment
    echo "Please provide the step-specific directory."
    exit 1
fi

dir=$(realpath "$1");
step=$(basename $dir);

echo "" &&
echo "Loading image" &&
docker load -i $dir/docker/step.tar &&
image_id=$(docker images --filter=reference=$step --format "{{.ID}}")
echo "" &&
echo "Running the step" &&
docker run --rm -v $dir:/app $image_id 2>&1 | tee $dir/docker/$step.o &&
echo "" &&
echo "Removing image" &&
docker rmi $image_id &&
echo "" &&
echo "*** finished ***" &&
echo ""