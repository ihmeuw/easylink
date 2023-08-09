# linker

NOTE: "linker" is a temporary name and will change when the official one is
decided on.

linker is a framework that allows users to build and run highly configurable
entity resolution (ER) pipelines.

## Installation

- Install docker
    - Mac: https://docs.docker.com/desktop/install/mac-install/
    - Windows: https://docs.docker.com/desktop/install/windows-install/
- Clone this repo
    - ssh: `git clone git@github.com:ihmeuw/linker.git`
    - https: `git clone https://github.com/ihmeuw/linker.git`


## Running a pipeline

NOTE: Each pipeline step gets its own sub-folder in the "linker/" parent
directory, e.g. "linker/pvs_like_case_study_sample_data/".

```
$ sh <path-to-repo>/run_step.sh <step>
$ # e.g. from linker/, `sh run_step.sh pvs_like_case_study_sample_data`
```

This will build the image from the Dockerfile, run the step in the container, and save the output file in the results folder.

### Requirements

TBD

## Creating a docker image to be shared

The docker images are built from the Dockerfile. There may be unique situations,
however, where you want to share a pre-built image. Images can be quite large and so distributing via pypi is not an option. One common method of sharing is to use the docker repository. Another one is to save the built image as an executable .tar file which can be distributed like any other file.

To create an image .tar file from a Dockerfile,  first ensure Docker is installed and then:

```
$ cd linker/pvs_like_case_study_sample_data/
$ docker build -t linker:pvs_like_case_study_sample_data .
$ # Convert to .tar.gz
$ docker save linker | gzip > image.tar.gz
$ # Remove the image
$ docker rmi linker
```

You should now have a step.tar image file alongside the Dockerfile which can be
used to spin up the container.