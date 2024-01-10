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

Docker image binaries can be built from a Dockerfile. For example, to create a
compressed image .tar.gz file:

```
$ cd <PATH-TO-DOCKERFILE-PARENT-DIRECTORY>
$ # build the image
$ sudo docker build -t linker:<IMAGE-NAME> .
$ # save as compressed tarball
$ sudo docker save linker:<IMAGE-NAME> | gzip > <IMAGE-NAME>.tar.gz
$ # remove the image
$ sudo docker rmi linker:<IMAGE-NAME>
```

You can use the `-f` option to build a dockerfile from a different location
(including a different filename than 'Dockerfile'):

```
sudo docker build -t linker:<IMAGE-NAME> <PATH-TO-DOCKERFILE>
```

You should now have an image file named `<IMAGE-NAME>.tar.gz` alongside the Dockerfile which can be used to spin up the container.

Note that it may be occasionally required to clean up unused data to make room for building
images: `sudo docker system prune`.

## Creating a singularity image to be shared

Singularity image files can be created from a Singularity file. For example:

```
$ cd <PATH-TO-SINGULARITY-FILE-PARENT-DIRECTORY>
$ # build the image
$ singularity build --force <IMAGE-NAME>.sif Singularity
```

Alternatively, a Docker binary can be converted to a Singularity image file:

```
$ singularity build --force <IMAGE-NAME>.sif docker-archive://$(pwd)/<IMAGE-NAME>.tar.gz
```
