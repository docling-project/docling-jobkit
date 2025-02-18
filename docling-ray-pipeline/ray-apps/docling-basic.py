r"""
This is basic ray app that uses docling to convert documents.
"""


import time
import ray
import argparse
from docling.document_converter import DocumentConverter


# Simple set of inputs that we want to process
input_docs = [
    "https://arxiv.org/pdf/2501.17887",
    "https://arxiv.org/pdf/2408.09869",
    "https://arxiv.org/pdf/2501.17887",
    "https://arxiv.org/pdf/2408.09869"
]


def print_runtime(input_data):
    print(*input_data, sep="\n")


# This is executed on ray-worker
@ray.remote
def convert_doc(index, db_ref):
    converter = DocumentConverter()
    result = converter.convert(db_ref[index])
    outputs = result.document.export_to_markdown()  # output: "## Docling Technical Report[...]"
    return index, outputs


# This is executed on the ray-head
def main(args):
    ## Init stuff
    ray.init(local_mode=False)

    db_object_ref = ray.put(input_docs)

    object_references = [
        convert_doc.remote(index, db_object_ref) for index in range(4)
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
        description="Basic docling ray app"
    )

    args = parser.parse_args()
    main(args)