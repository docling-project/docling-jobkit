# Docling Jobkit

Running a distributed job processing documents with Docling.


## How to use it

## Kubeflow pipeline with Docling Jobkit

### Using Kubeflow pipeline web dashboard UI

1. From the main page, open "Pipelines" section on the left
2. Press on "Upload pipeline" button at top-right
3. Give pipeline a name and in "Upload a file" menu point to location of `docling-jobkit/docling_jobkit/kfp_pipeline/docling-s3in-s3out.yaml` file
4. Now you can press "Create run" button at the top-right to create an instance of the pipeline
5. Customize required inputs according to provided examples and press "Start" to start pipeline run

### Using OpenshiftAI web dashboard UI
1. From the main page of Red Hat Openshift AI open "Data Science Pipelines -> Pipelines" section on the left side
2. Switch "Project" to namespace where you plan to run pipelines
3. Press on "Import Pipeline", provide a name and upload the `docling-jobkit/docling_jobkit/kfp_pipeline/docling-s3in-s3out.yaml` file
4. From the selected/created pipeline interface, you can start new run by pressing "Actions -> Create Run"
5. Customize required inputs according to provided examples and press "Start" to start pipeline run
 
### Customizing pipeline to specifics of your infrastructure

Some customizations, such as paralelism level, node selector or tollerations, require changing source script and compiling new yaml manifest.
Source script is located at `docling-jobkit/docling_jobkit/kfp_pipeline/docling-s3in-s3out.py`.

If you use web UI to run pipelines, then python script need to be compiled into yaml and new version of yaml uploaded to pipeline.
For example, you can use poetry to handle python environment and run following command:
``` sh
uv run python semantic-ingest-batches.py
```
The yaml file will be generated in the local folder from where you execute command.
Now in the web UI, you can open existing pipeline and upload new version of the script using "Upload version" at top-right.

By defaul, paralelism is set to 20 instances, this can be change in the source `docling-jobkit/docling_jobkit/kfp_pipeline/docling-s3in-s3out.py` script, look for this line `with dsl.ParallelFor(batches.outputs["batch_indices"], parallelism=20) as subbatch:`.

By default, the resources requests/limits for the document convertion component are set to following:
``` py
converter.set_memory_request("1G")
converter.set_memory_limit("7G")
converter.set_cpu_request("200m")
converter.set_cpu_limit("1")
```

By default, the resource request/limit are not set for the nodes with GPU, you can uncomment following lines in the `inputs_s3in_s3out` pipeline function to enable it:
``` py
converter.set_accelerator_type("nvidia.com/gpu")
converter.set_accelerator_limit("1")
```

The node selector and tollerations can be enabled with following commands, customize actual values to your infrastructure:
``` py
from kfp import kubernetes

kubernetes.add_node_selector(
  task=converter,
  label_key="nvidia.com/gpu.product",
  label_value="NVIDIA-A10",
)

kubernetes.add_toleration(
  task=converter,
  key="gpu_compute",
  operator="Equal",
  value="true",
  effect="NoSchedule",
)
```

### Running pipeline programatically

At the end of the script file you can find an example code for submitting pipeline run programatically.
You can provide your custom values as environment variables in an `.env` file and bind it during execution:
``` sh
uv run --env-file .env python docling-s3in-s3out.py
```


## Ray runtime with Docling Jobkit


Make sure your Ray cluster has `docling-jobkit` installed, then submit the job.

```sh
ray job submit --no-wait --working-dir . --runtime-env runtime_env.yml -- docling-ray-job
```

### Custom runtime environment


1. Create a file `runtime_env.yml`:

    ```yaml
    # Expected environment if clean ray image is used. Take into account that ray worker can timeout before it finishes installing modules.
    pip:
    - docling-jobkit
    ```


2. Submit the job using the custom runtime env: 

    ```sh
    ray job submit --no-wait --runtime-env runtime_env.yml -- docling-ray-job
    ```

More examples and customization are provided in [docs/ray-job/](docs/ray-job/README.md).


### Custom image with all dependencies

Coming soon. Initial instruction from [OpenShift AI docs](https://docs.redhat.com/en/documentation/red_hat_openshift_ai_self-managed/2-latest/html/working_with_distributed_workloads/managing-custom-training-images_distributed-workloads#creating-a-custom-training-image_distributed-workloads).


## Get help and support

Please feel free to connect with us using the [discussion section](https://github.com/docling-project/docling/discussions) of the main [Docling repository](https://github.com/docling-project/docling).

## Contributing

Please read [Contributing to Docling Serve](https://github.com/docling-project/docling-jobkit/blob/main/CONTRIBUTING.md) for details.

## References

If you use Docling in your projects, please consider citing the following:

```bib
@techreport{Docling,
  author = {Deep Search Team},
  month = {1},
  title = {Docling: An Efficient Open-Source Toolkit for AI-driven Document Conversion},
  url = {https://arxiv.org/abs/2501.17887},
  eprint = {2501.17887},
  doi = {10.48550/arXiv.2501.17887},
  version = {2.0.0},
  year = {2025}
}
```

## License

The Docling Serve codebase is under MIT license.

## LF AI & Data

Docling is hosted as a project in the [LF AI & Data Foundation](https://lfaidata.foundation/projects/).

### IBM ❤️ Open Source AI

The project was started by the AI for Knowledge team at IBM Research Zurich.
