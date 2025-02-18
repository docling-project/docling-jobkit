# ray-docling docker image

Repository for an image with ray and docling.

Building and pushing image manually:
```
DOCKER_BUILDKIT=1 docker build --platform linux/amd64 --progress=plain --tag icr.io/ibm-deepsearch/ray-docling:v1.0 .
ibmcloud login --sso
ibmcloud cr login
docker push icr.io/ibm-deepsearch/ray-docling:v1.0
```


# using image for ray cluster

This image primary purpose is to be used as a base for the ray applications utilizing docling.
Follow guidelines in the `docling-ray-pipeline/README.md` for deployment of ray cluster and ray apps.


