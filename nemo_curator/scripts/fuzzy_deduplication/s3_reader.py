# Copyright (c) 2024, NVIDIA CORPORATION.  All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os

import dask
import dask.dataframe as dd

from nemo_curator.datasets import DocumentDataset

dask.config.set({"dataframe.backend": "cudf"})
import time

import boto3

s3_client = boto3.client("s3")


def extract_s3_info(s3_uri):
    if not s3_uri.startswith("s3://"):
        raise ValueError("Invalid S3 URI")
    # Remove the 's3://' prefix
    path = s3_uri[5:]
    # Split the URI into bucket and prefix
    parts = path.split("/")
    # Extract bucket name and prefix
    bucket_name = parts[0]
    prefix = "/".join(parts[1:])
    return bucket_name, prefix


def compute_minhashes_s3_uri(minhasher, s3_input_minhash_dirs, s3_output_minhash_dir):
    t0 = time.time()
    for s3_input_minhash_dir in s3_input_minhash_dirs:
        bucket_name, prefix = extract_s3_info(s3_input_minhash_dir)
        paginator = s3_client.get_paginator("list_objects_v2")
        count_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix).search(
            "KeyCount"
        )
        total_keys = sum(count for count in count_iterator)

        t1 = time.time()
        df = dd.read_parquet(s3_input_minhash_dir, aggregate_files=True)
        print(f"Reading s3 path {s3_input_minhash_dir} :- {total_keys} files")
        res = minhasher(DocumentDataset(df)).df
        print("Writing to s3 directory")
        res.to_parquet(s3_output_minhash_dir)
        elapsed_time = time.time() - t1
        # Print the elapsed time with a descriptive message
        print(f"Elapsed time for {s3_input_minhash_dir}: {elapsed_time:.2f} seconds")
    total_elapsed_time = time.time() - t0
    print(f"Elapsed time for {s3_input_minhash_dirs}: {total_elapsed_time:.2f} seconds")


def compute_minhashes_local_parquet(minhasher, input_minhash_dir, output_minhash_dir):
    t0 = time.time()
    df = dd.read_parquet(input_minhash_dir, aggregate_files=True)
    print(f"Reading local parquet dir {input_minhash_dir}")
    res = minhasher(DocumentDataset(df)).df
    print("Writing to output directory")
    res.to_parquet(output_minhash_dir)
    elapsed_time = time.time() - t0
    # Print the elapsed time with a descriptive message
    print(f"Elapsed time: {elapsed_time:.2f} seconds")


import os

import dask
import dask.dataframe as dd

dask.config.set({"dataframe.backend": "cudf"})


def list_files_with_full_path(directory):
    file_list = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            full_path = os.path.join(root, file)
            file_list.append(full_path)
    return file_list


files = list_files_with_full_path("/nemo-workspace/redpajama_sharded_add_id/c4/")
files = files[6000:]

for file in files:
    print(f"Reading {file}")
    df = dd.read_json(file)
    filename = file.split("/")[-1]
    df.to_parquet(
        "/nemo-workspace/redpajama_parquet/c4/",
        name_function=lambda x: f"part-{x}.{filename.split('.')[0]}.parquet",
    )
