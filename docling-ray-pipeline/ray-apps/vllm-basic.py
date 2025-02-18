r"""
This is basic ray app that uses vllm to spawn instance of the model and do simple inference.
VLLM instanciate model on the worker node. The expectation is that only workers nodes are using GPU,
while head node is not blocking GPU use and can be located on a completely separate k8s node.
"""


import time
import ray
import argparse
import docling
from vllm import LLM, SamplingParams
from vllm.platforms import current_platform


# Simple set of inputs that we want to process
prompts = [
    "Hello, my name is",
    "The president of the United States is",
    "The capital of France is",
    "The future of AI is",
]
sampling_params = SamplingParams(temperature=0.8, top_p=0.95)


def print_runtime(input_data):
    print(*input_data, sep="\n")


# This is executed on ray-worker
@ray.remote(num_gpus=1) # Here we must set how many GPUs in ray terms we expect, the value can be fractional
def do_inference(index, db_ref):
    print("Checking gpu on worker node:")
    print(current_platform)
    print(current_platform.get_device_capability())
    print(current_platform.get_device_name())
    llm = LLM(model="facebook/opt-125m")
    outputs = llm.generate(db_ref[index], sampling_params)
    return index, outputs


# This is executed on the ray-head
def main(args):
    ## Init stuff
    ray.init(local_mode=False)

    '''
    # Head node doesn't need and won't utilize GPU so following prints can cause error
    print("Checking gpu on head node:")
    print(current_platform)
    print(current_platform.get_device_capability())
    print(current_platform.get_device_name())
    '''

    db_object_ref = ray.put(prompts)

    object_references = [
        do_inference.remote(index, db_object_ref) for index in range(4)
    ]
    all_data = []

    while len(object_references) > 0:
        finished, object_references = ray.wait(
            object_references, timeout=7.0
        )
        data = ray.get(finished)
        print_runtime(data)
        all_data.extend(data)

    print_runtime(all_data)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Basic vllm ray app"
    )

    args = parser.parse_args()
    main(args)