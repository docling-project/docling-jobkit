#FROM rayproject/ray:2.42.1 as ray-runtime
FROM quay.io/modh/ray:2.35.0-py311-rocm62

# Base
USER 0

COPY requirements.txt ./

RUN pip install -U pip --no-cache-dir -r requirements.txt && \
    rm -f requirements.txt && \
    yum clean all && rm -rf /var/cache/yum