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


def compute_minhashes(minhasher, s3_input_minhash_dir, s3_output_minhash_dir):
    bucket_name, prefix = extract_s3_info(s3_input_minhash_dir)
    paginator = s3_client.get_paginator("list_objects_v2")
    count_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix).search(
        "KeyCount"
    )
    total_keys = sum(count for count in count_iterator)

    t0 = time.time()
    df = dd.read_parquet(s3_input_minhash_dir)
    print(f"Reading s3 path {s3_input_minhash_dir} :- {total_keys} files")
    res = minhasher(DocumentDataset(df)).df
    print("Writing to s3 directory")
    res.to_parquet(s3_output_minhash_dir)
    elapsed_time = time.time() - t0
    # Print the elapsed time with a descriptive message
    print(f"Elapsed time: {elapsed_time:.2f} seconds")
