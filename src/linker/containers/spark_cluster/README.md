# spark_cluster container
NOTE: Use of `linker build-spark-cluster` currently requires building an image.sif from this directory.

This is done by running the following commands this directory:

```
# build the image
$ sudo docker build -t linker:sparkbuilder .
# save as compressed tarball
$ sudo docker save linker:sparkbuilder | gzip > image.tar.gz
# remove the image
$ sudo docker rmi linker:sparkbuilder
# convert the image from the docker image
$ singularity build --force image.sif docker-archive://$(pwd)/image.tar.gz
```