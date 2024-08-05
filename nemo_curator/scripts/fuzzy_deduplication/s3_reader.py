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
import time

import dask
import dask.dataframe as dd

from nemo_curator.datasets import DocumentDataset

dask.config.set({"dataframe.backend": "cudf"})


def compute_minhashes(minhasher, s3_input_minhash_dir, s3_output_minhash_dir):
    t0 = time.time()
    df = dd.read_parquet(s3_input_minhash_dir)
    print("Reading s3 directory")
    res = minhasher(DocumentDataset(df)).df
    print("Writing to s3 directory")
    res.to_parquet(s3_output_minhash_dir)
    elapsed_time = time.time() - t0

    # Print the elapsed time with a descriptive message
    print(f"Elapsed time: {elapsed_time:.2f} seconds")
