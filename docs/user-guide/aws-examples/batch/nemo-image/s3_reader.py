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
from nemo_curator.log import create_logger
from nemo_curator.datasets import DocumentDataset
import re

dask.config.set({"dataframe.backend": "cudf"})
import time
import subprocess

logger = create_logger(
    rank=0, log_file=os.path.join("./", "s3-reader.log"), name="minhash_with_s3_log"
)


def get_dir_size(bucket_path):
    command = [
        "aws",
        "s3",
        "ls",
        "--summarize",
        "--human-readable",
        "--recursive",
        bucket_path,
    ]
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        total_size = re.search(r"Total Size: ([\d,]+)", result.stdout)
        total_objects = re.search(r"Total Objects: (\d+)", result.stdout)
        logger.info(f"Total Size: {total_size.group(1)} bytes")
        logger.info(f"Total Objects: {total_objects.group(1)}")
        return total_size.group(1)
    except subprocess.CalledProcessError as e:
        logger.error(f"Error: {e.stderr}")


def get_len(df):
    t1 = time.time()
    df = df.map_partitions(
        lambda x: x,
        meta=df._meta,
        enforce_metadata=False,
    )
    logger.info(f"computing length: {len(df)}")
    logger.info(f"Length operation using took {(time.time()-t1):2f} seconds")


def compute_minhashes_s3_uri(
    minhasher, s3_input_minhash_dirs, s3_output_minhash_dir, filesystem, readop
):
    logger.info(
        f"Starting compute_minhashes_s3_uri with parameters: "
        f"Minhasher: {minhasher}, "
        f"S3 Input Minhash Directories: {s3_input_minhash_dirs}, "
        f"S3 Output Minhash Directory: {s3_output_minhash_dir}, "
        f"Filesystem: {filesystem}, "
        f"Read only Operation by just calculating length operation: {readop}"
    )
    t0 = time.time()
    for s3_input_minhash_dir in s3_input_minhash_dirs:
        total_keys = get_dir_size
        logger.info(f"Reading s3 path {s3_input_minhash_dir} :- {total_keys} files")

        t1 = time.time()
        if filesystem == "arrow":
            df = dd.read_parquet(s3_input_minhash_dir, filesystem="arrow")
        else:
            df = dd.read_parquet(
                s3_input_minhash_dir, blockSize="1GB", aggregate_files=True
            )
        if readop:
            get_len(df)
            continue
        res = minhasher(DocumentDataset(df)).df
        out_dir = s3_output_minhash_dir + s3_input_minhash_dir.split("/")[-1]
        logger.info(f"Writing to s3 directory:  {out_dir}")
        res.to_parquet(out_dir, write_index=False)
        logger.info(
            f"Elapsed time for {s3_input_minhash_dir} {(time.time() - t1):.3f} seconds"
        )
    logger.info(
        f"Elapsed time for {s3_input_minhash_dir} {(time.time() - t0):.3f} seconds"
    )


def compute_minhashes_local_parquet(minhasher, input_minhash_dir, output_minhash_dir):
    t0 = time.time()
    df = dd.read_parquet(input_minhash_dir, aggregate_files=True)
    logger.info(f"Reading local parquet dir {input_minhash_dir}")
    res = minhasher(DocumentDataset(df)).df
    logger.info("Writing to output directory")
    res.to_parquet(output_minhash_dir)
    elapsed_time = time.time() - t0
    # Print the elapsed time with a descriptive message
    logger.info(f"Elapsed time: {elapsed_time:.2f} seconds")
