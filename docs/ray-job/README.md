# Example Docling Ray Job


### Complete `runtime_env.yml`

```yaml
working_dir: "./"

env_vars:
  OMP_NUM_THREADS: "4"
  S3_SOURCE_ACCESS_KEY: "myaccesskey"
  S3_SOURCE_SECRET_KEY: "mysecretkey"
  S3_SOURCE_ENDPOINTS: "mys3service.com"
  S3_SOURCE_BUCKET: "docling-pipelines-source"
  S3_SOURCE_PREFIX: "documents-to-convert"
  S3_SOURCE_SSL: "True"
  S3_TARGET_ACCESS_KEY: "myaccesskey"
  S3_TARGET_SECRET_KEY: "mysecretkey"
  S3_TARGET_ENDPOINTS: "mys3service.com"
  S3_TARGET_BUCKET: "docling-pipelines-results"
  S3_TARGET_PREFIX: "converted-documents"
  S3_TARGET_SSL: "True"
  BATCH_SIZE: "20"
  
  # Docling conversion settings
  SETTINGS_DO_OCR: "True"
  SETTINGS_OCR_KIND: "easyocr"
  SETTINGS_DO_TABLE_STRUCTURE: "True"
  SETTINGS_TABLE_STRUCTURE_MODE: "fast"
  SETTINGS_GENERATE_PAGE_IMAGES: "True"


# Expected environment if clean ray image is used. Take into account that ray worker can timeout before it finishes installing modules.
pip:
  - docling-jobkit
  - --index-url=https://download.pytorch.org/whl/cpu
```
