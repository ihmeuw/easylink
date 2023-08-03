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

If the image file(s) (.tar) have not been provided, you can create them from the
Dockerfile. refer to the `Creating a docker image to be shared` section, below.

Assuming you have the image .tar file, you can run the pipeline by:

```
$ 
$ sh <path-to-repo>/run_step.sh <step>
$ 
```

### Requirements

TBD

## Creating a docker image to be shared

Docker images can be quite large and so distributing via pypi is not an option. One common method of sharing is to use the docker repository. However, for now, we instead save the built images as an executable .tar file which can be distributed like any other file.

In the event you do not have the image file(s), you can create them from the
Dockerfile. To do so, first ensure Docker is installed and then:

```
$ cd linker/pvs_like_case_study_sample_data/
$ docker build -t pvs_like_case_study_sample_data --no-cache .
$ # Convert to .tar
$ docker save -o step.tar pvs_like_case_study_sample_data
$ # Remove the image
$ docker rmi pvs_like_case_study_sample_data
```

You should now have a step.tar image file alongside the Dockerfile which can be
used to spin up the container.