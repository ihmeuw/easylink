# spark_cluster container
NOTE: Spinning up a spark cluster using `easylink` currently requires building an image from this directory.

This is done by running the following commands from this directory:

```
# build the image
$ sudo docker build -t easylink:sparkbuilder .
# save as compressed tarball
$ sudo docker save easylink:sparkbuilder | gzip > spark_cluster.tar.gz
# remove the image
$ sudo docker rmi easylink:sparkbuilder
# convert the image from the docker image
$ singularity build --force spark_cluster.sif docker-archive://$(pwd)/spark_cluster.tar.gz
```