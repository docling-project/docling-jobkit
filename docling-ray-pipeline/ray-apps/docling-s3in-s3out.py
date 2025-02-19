r"""
This is basic ray app that uses docling to convert documents.
"""


import time
import ray
import argparse
import os
import json
import yaml
import boto3
from botocore.exceptions import ClientError
from docling.document_converter import DocumentConverter
from docling.datamodel.base_models import ConversionStatus


# Load credentials
s3_source_access_key = os.environ['S3_SOURCE_ACCESS_KEY']
s3_source_secret_key = os.environ['S3_SOURCE_SECRET_KEY']
s3_source_endpoint = os.environ['S3_SOURCE_ENDPOINTS']
s3_source_bucket = os.environ['S3_SOURCE_BUCKET']
s3_source_prefix = os.environ['S3_SOURCE_PREFIX']
s3_target_access_key = os.environ['S3_TARGET_ACCESS_KEY']
s3_target_secret_key = os.environ['S3_TARGET_SECRET_KEY']
s3_target_endpoint = os.environ['S3_TARGET_ENDPOINTS']
s3_target_bucket = os.environ['S3_TARGET_BUCKET']
s3_target_prefix = os.environ['S3_TARGET_PREFIX']
batch_size = int(os.environ['BATCH_SIZE'])


def count_s3_objects(s3_resource, bucket_name, prefix):
    bucket = s3_resource.Bucket(bucket_name)
    count_obj = sum(1 for _ in bucket.objects.filter(Prefix=prefix))
    return count_obj


def get_keys_s3_objects_as_set(s3_resource, bucket_name, prefix):
    bucket = s3_resource.Bucket(bucket_name)
    folder_objects = list(bucket.objects.filter(Prefix=prefix))
    files_on_s3 = set()
    for file in folder_objects:
        files_on_s3.add(file.key)
    return files_on_s3


def strip_prefix_postfix(source_set, prefix = '', extension = ''):
    output = set()
    for key in source_set:
        output.add(key.replace(extension, '').replace(prefix, ''))
    return output


def print_runtime(input_data):
    print(*input_data, sep="\n")


# This is executed on ray-worker
@ray.remote
def convert_doc(index, db_ref):
    USE_V2 = True
    converter = DocumentConverter()
    s3_target = boto3.resource(
            's3', endpoint_url = 'https://' + s3_target_endpoint,
            aws_access_key_id = s3_target_access_key,
            aws_secret_access_key = s3_target_secret_key
        )
    outputs = []
    for url in db_ref[index]:
        conv_res = converter.convert(url)
        if conv_res.status == ConversionStatus.SUCCESS:
            doc_filename = conv_res.input.file.stem
            if USE_V2:
                # Export Docling document format to JSON:
                target_key = f"{s3_target_prefix}/json/{doc_filename}.json"
                data = json.dumps(conv_res.document.export_to_dict())
                s3_target.Object(s3_target_bucket, target_key).put(Body=data)

                # Export Docling document format to YAML:
                target_key = f"{s3_target_prefix}/yaml/{doc_filename}.yaml"
                data = yaml.safe_dump(conv_res.document.export_to_dict())
                s3_target.Object(s3_target_bucket, target_key).put(Body=data)

                # Export Docling document format to doctags:
                target_key = f"{s3_target_prefix}/doctags/{doc_filename}.doctags.txt"
                data = conv_res.document.export_to_document_tokens()
                s3_target.Object(s3_target_bucket, target_key).put(Body=data)

                # Export Docling document format to markdown:
                target_key = f"{s3_target_prefix}/md/{doc_filename}.md"
                data = conv_res.document.export_to_markdown()
                s3_target.Object(s3_target_bucket, target_key).put(Body=data)

                # Export Docling document format to text:
                target_key = f"{s3_target_prefix}/txt/{doc_filename}.txt"
                data = conv_res.document.export_to_markdown(strict_text=True)
                s3_target.Object(s3_target_bucket, target_key).put(Body=data)

                outputs.append(f"{doc_filename} - SUCCESS")

        elif conv_res.status == ConversionStatus.PARTIAL_SUCCESS:
            outputs.append(f"{conv_res.input.file} - PARTIAL_SUCCESS")
        else:
            outputs.append(f"{conv_res.input.file} - FAILURE")
    return index, outputs


# This is executed on the ray-head
def main(args):
    ## Init stuff
    ray.init(local_mode=False)

    # Check inputs
    if (not s3_source_access_key) or (not s3_source_secret_key) or (not s3_target_access_key) or (not s3_target_secret_key):
        print("s3 source or target keys are missing")
        ray.shutdown()
    if (not s3_source_endpoint) or (not s3_target_endpoint):
        print("s3 source or target endpoint is missing")
        ray.shutdown()
    if (not s3_source_bucket) or (not s3_target_bucket):
        print("s3 source or target bucket is missing")
        ray.shutdown()
    if (s3_source_endpoint == s3_target_endpoint) and (s3_source_bucket == s3_target_bucket) and (s3_source_prefix == s3_target_prefix):
        print("s3 source and target are the same")
        ray.shutdown()
    if batch_size == 0:
        print("batch_size have to be higher than zero")
        ray.shutdown()
    

    # Init source and target s3 clients
    s3_source = boto3.resource(
        's3', endpoint_url = 'https://' + s3_source_endpoint,
        aws_access_key_id = s3_source_access_key,
        aws_secret_access_key = s3_source_secret_key
    )

    # Check that source is not empty
    source_count = count_s3_objects(s3_source, s3_source_bucket, s3_source_prefix + '/')
    if source_count == 0:
        print("s3 source is empty")
        ray.shutdown()
    source_objects_list = get_keys_s3_objects_as_set(s3_source, s3_source_bucket, s3_source_prefix)


    # Check if target contains anything
    s3_target = boto3.resource(
            's3', endpoint_url = 'https://' + s3_target_endpoint,
            aws_access_key_id = s3_target_access_key,
            aws_secret_access_key = s3_target_secret_key
        )
    converted_prefix = s3_target_prefix + "/json/"
    target_count = count_s3_objects(s3_target, s3_target_bucket, converted_prefix)
    print('Target contains json objects: ',target_count)
    if target_count != 0:
        print('Target contains objects, checking content...')

        # Collect target keys for iterative conversion
        existing_target_objects = get_keys_s3_objects_as_set(s3_target, s3_target_bucket, converted_prefix)

        # Filter-out objects that are already processed
        target_short_key_list = strip_prefix_postfix(existing_target_objects, prefix=converted_prefix, extension='.json')
        filtered_source_keys = []
        print("List of source keys:")
        for key in source_objects_list:
            print(key)
            clean_key = key.replace('.pdf', '').replace(s3_source_prefix + '/', '')
            if clean_key not in target_short_key_list:
                filtered_source_keys.append(key)
        
        print('Total keys: ', len(source_objects_list))
        print('Filtered keys to process: ', len(filtered_source_keys))

    # Generate pre-signed urls
    session = boto3.session.Session()
    s3_client = session.client(
        service_name = 's3',
        aws_access_key_id = s3_source_access_key,
        aws_secret_access_key = s3_source_secret_key,
        endpoint_url = 'https://' + s3_source_endpoint,
    )

    presigned_urls = []
    counter = 0
    sub_array = []
    array_lenght = len(filtered_source_keys)
    for idx, key in enumerate(filtered_source_keys):
        try:
            url = s3_client.generate_presigned_url(
                ClientMethod='get_object',
                Params={
                    'Bucket': s3_source_bucket,
                    'Key': key
                },
                ExpiresIn=3600
            )
        except ClientError as e:
            print(error(e))
        sub_array.append(url)
        counter += 1
        if counter == batch_size or (idx + 1) == array_lenght:
            presigned_urls.append(sub_array)
            sub_array = []
            counter = 0


    # Send payload to ray
    db_object_ref = ray.put(presigned_urls)

    object_references = [
        convert_doc.remote(index, db_object_ref) for index in range(len(presigned_urls))
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