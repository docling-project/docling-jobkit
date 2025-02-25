#FROM rayproject/ray:2.42.1 as ray-runtime
FROM quay.io/modh/ray:2.35.0-py311-rocm62

# Base
USER 0

COPY requirements.txt ./

RUN yum install -y python3-pip && \
    pip install -U pip --no-cache-dir -r requirements.txt && \
    rm -f requirements.txt && \
    yum clean all && rm -rf /var/cache/yum
    # Fix permissions to support pip in Openshift environments \
#    chmod -R g+w /home/ray


# RUN apt-get update && apt-get install -y --no-install-recommends python3-pip && \
#     pip install -U pip --no-cache-dir -r requirements.txt && \
#     rm -f requirements.txt && \
#     # Fix permissions to support pip in Openshift environments \
#     chmod -R g+w /home/ray

#ENV PATH="$PATH:/home/ray/.local/bin"
#ENV PATH="$PATH:/usr/local/nvidia/bin:/usr/local/cuda/bin"

#RUN chgrp -R 0 /home && \
#    chmod -R g=u /home