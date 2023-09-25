# linker

NOTE: "linker" is a temporary name and will change when the official one is
decided on.

linker is a framework that allows users to build and run highly configurable
entity resolution (ER) pipelines.

## Installation

- Install docker
    - Mac: https://docs.docker.com/desktop/install/mac-install/
    - Windows: https://docs.docker.com/desktop/install/windows-install/
- Clone and install this repo
```
$ cd <path/to/repositories/>
$ git clone git@github.com:ihmeuw/linker.git
$ # OR `git clone https://github.com/ihmeuw/linker.git`
$ cd linker
$ pip install .
```

## Running a pipeline

```
$ linker run <PIPELINE-SPECIFICATION>
$ # e.g. `linker run ~/repos/linker/src/linker/pipelines/pipeline.yaml`
```

For help, please use `linker --help`

### Requirements

TBD

## Creating a docker image to be shared

The docker images are built from the Dockerfile. There may be unique situations,
however, where you want to share a pre-built image. Images can be quite large and so distributing via pypi is not an option. One common method of sharing is to use the docker repository. Another one is to save the built image as an executable .tar file which can be distributed like any other file.

To create an image .tar file from a Dockerfile, first ensure Docker is installed
then convert like the following example for some `<implementation>`:

```
$ cd <path/to/repositories>/linker/steps/<step_name>/implementations/<implementation>
$ # build the image
$ sudo docker build -t linker:<implementation> .
$ # save as compressed tarball
$ sudo docker save linker:<implementation> | gzip > image.tar.gz
$ # remove the image
$ sudo docker rmi linker:<implementation>
```

You should now have an image file named `image.tar.gz` alongside the Dockerfile 
which can be used to spin up the container.


## Creating a singularity image to be shared

Singularity images should be built from a docker image created using the
instructions in the section above. With an `image.tar.gz` docker image,
you can create a SIF using:

```
$ cd <path/to/repositories>/linker/steps/<step_name>/implementations/<implementation>
$ # convert the image from the docker image
$ singularity build --force image.sif docker-archive://$(pwd)/image.tar.gz
```
