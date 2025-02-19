#!/usr/bin/env bash

set -euo pipefail

#export RAY_RUNTIME_ENV_IGNORE_GITIGNORE=1

export RAY_ADDRESS=http://localhost:8265

ray job submit --no-wait --working-dir . --runtime-env runtime_env.yml -- python ray-apps/docling-s3in-s3out.py