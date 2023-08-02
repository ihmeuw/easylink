# linker

...

## Creating a docker image to be shared

Docker images can be quite large and so distributing via pypi is not an option. One common method of sharing is to use the docker repository. However, for now, we instead save the built images as an executable .tar file which can be distributed like any other file.

To build an image .tar file, first install Docker on your machine, then:
```
$ cd <PATH-TO-DOCKERFILE>
$ docker build -t <IMAGE-NAME>:<IMAGE-TAG> --no-cache .
$ docker save -o step.tar <IMAGE-NAME>:<IMAGE-TAG>
```
