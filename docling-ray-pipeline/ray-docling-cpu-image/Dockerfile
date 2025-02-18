FROM rayproject/ray:2.42.1 as ray-runtime

# Base
USER 0

COPY requirements.txt ./

RUN apt-get update && apt-get install -y --no-install-recommends python3-pip && \
    pip install -U pip --no-cache-dir -r requirements.txt && \
    rm -f requirements.txt && \
    # Fix permissions to support pip in Openshift environments \
    chmod -R g+w /home/ray

ENV PATH="$PATH:/home/ray/.local/bin"
ENV PATH="$PATH:/usr/local/nvidia/bin:/usr/local/cuda/bin"

RUN chgrp -R 0 /home && \
    chmod -R g=u /home