# Docling Jobkit

Running a distributed job processing documents with Docling.

 > [!NOTE]
> This is an unstable draft implementation which will quickly evolve.


## How to use it

Make sure your Ray cluster has `docling-jobkit` installed, then submit the job.

```sh
ray job submit --no-wait --working-dir . --runtime-env runtime_env.yml -- docling-ray-job
```

## Ray runtime with Docling Jobkit


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
