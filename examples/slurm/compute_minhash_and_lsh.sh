#! /bin/bash

#SBATCH --job-name=nemo-curator:example-script
#SBATCH --nodes=1
#SBATCH --exclusive
#SBATCH --cpus-per-task=128
#SBATCH --time=08:00:00

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

# =================================================================
# Begin easy customization
# =================================================================

# Base directory for all SLURM job logs and files
# Does not affect directories referenced in your script
export BASE_JOB_DIR=/shared/nemo-curator-jobs

# Main script to run
# In the script, Dask must connect to a cluster through the Dask scheduler
# We recommend passing the path to a Dask scheduler's file in a
# nemo_curator.utils.distributed_utils.get_client call like the examples
export DEVICE='gpu'
export SCRIPT_PATH=/shared/NeMo-Curator/nemo_curator/scripts/fuzzy_deduplication/minhash_lsh.py

# Container parameters
export CONTAINER_IMAGE=/shared/nemo.sqsh
# Make sure to mount the directories your script references
export BASE_DIR="/shared/NeMo-Curator/"
export MOUNTS="${BASE_DIR}:${BASE_DIR},/shared:/shared"
# Below must be path to entrypoint script on your system
export CONTAINER_ENTRYPOINT=$BASE_DIR/examples/slurm/container-entrypoint.sh

# Network interface specific to the cluster being used
export INTERFACE=ens5
export PROTOCOL=tcp

# CPU related variables
# 0 means no memory limit
export CPU_WORKER_MEMORY_LIMIT=0

# GPU related variables
export RAPIDS_NO_INITIALIZE="1"
export CUDF_SPILL="1"
export RMM_SCHEDULER_POOL_SIZE="1GB"
export RMM_WORKER_POOL_SIZE="18GiB"
export LIBCUDF_CUFILE_POLICY=OFF

# =================================================================
# End easy customization
# =================================================================

# Function to run a script inside the container
run_script() {
    local script_command=$1
    local log_dir=$2
    local profiles_dir=$3

    export LOGDIR=$log_dir
    export PROFILESDIR=$profiles_dir
    export SCHEDULER_FILE=$LOGDIR/scheduler.json
    export SCHEDULER_LOG=$LOGDIR/scheduler.log
    export DONE_MARKER=$LOGDIR/done.txt

    mkdir -p $LOGDIR
    mkdir -p $PROFILESDIR

    srun --nodes=1 \
         --exclusive \
         --container-mounts=${MOUNTS} \
         --container-image=${CONTAINER_IMAGE} \
         --output=$LOGDIR/slurm-%j.out \
         --error=$LOGDIR/slurm-%j.err \
         ${CONTAINER_ENTRYPOINT} \
         bash -c "$script_command"
}

# Run compute_minhashes.py
#
export SCRIPT_PATH=/shared/NeMo-Curator/nemo_curator/scripts/fuzzy_deduplication/compute_minhashes.py
export SCRIPT_COMMAND="
    export PYTHONPATH=/shared/NeMo-Curator:\$PYTHONPATH;
    export DASK_DATAFRAME__QUERY_PLANNING=False;
    time python \$SCRIPT_PATH \
    --input-data-dirs \
    s3://nemo-curator-paul/redpajama_parquet/arxiv \
    --input-json-id-field adlr_id \
    --input-json-text-field text \
    --files-per-partition 2 \
    --output-minhash-dir s3://nemo-curator-paul/compute_minhashes_1node_parquet_s3_benchmark \
    --log-dir /shared/nemo-curator-jobs/compute_minhashes/logs \
    --profile-path /shared/nemo-curator-jobs/compute_minhashes/profiles \
    --scheduler-file \$SCHEDULER_FILE
    "

run_script "$SCRIPT_COMMAND" "$BASE_JOB_DIR/compute_minhashes/logs" "$BASE_JOB_DIR/compute_minhashes/profiles"

# Run minhash_lsh.py
#export SCRIPT_PATH=/shared/NeMo-Curator/nemo_curator/scripts/fuzzy_deduplication/minhash_lsh.py
#export SCRIPT_COMMAND="
#    export PYTHONPATH=/shared/NeMo-Curator:\$PYTHONPATH;
#    export DASK_DATAFRAME__QUERY_PLANNING=False;
#    time python \$SCRIPT_PATH \
#    --input-data-dirs \
#    s3://nemo-curator-paul/compute_minhashes_1node_s3_benchmark/c4 \
#    --buckets-per-shuffle 3 \
#    --output-bucket-dir s3://nemo-curator-paul/minhash_lsh_1node_s3_benchmark \
#    --log-dir /shared/nemo-curator-jobs/minhash_lsh/logs \
#    --profile-path /shared/nemo-curator-jobs/minhash_lsh/profiles \
#    --scheduler-file \$SCHEDULER_FILE
#    "
#
#run_script "$SCRIPT_COMMAND" "$BASE_JOB_DIR/minhash_lsh/logs" "$BASE_JOB_DIR/minhash_lsh/profiles"
