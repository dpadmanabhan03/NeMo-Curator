#!/bin/bash
kubectl apply -f eks-batch-setup/

eksctl create iamidentitymapping \
    --cluster nemo-eks-cluster \
    --arn "arn:aws:iam::354625738399:role/AWSServiceRoleForBatch" \
    --username aws-batch \
    -p nim-eks-user

# Create compute environment
aws batch create-compute-environment \
    --cli-input-json file://compute-environment.json --region us-west-2

# Create job queue
aws batch create-job-queue \
    --cli-input-json file://job-queue.json

# Register job definition
aws batch register-job-definition \
    --cli-input-json file://aws-batch-infra/job-definitions/compute_minhashes-s3-csi-job.json \
    --region us-west-2

aws batch register-job-definition \
    --cli-input-json file://aws-batch-infra/job-definitions/compute-minhashes-s3-read_parquet-job.json \
    --region us-west-2

aws batch register-job-definition \
    --cli-input-json file://aws-batch-infra/job-definitions/minhash-lsh-s3-read-parquet.json \
    --region us-west-2


# s3 csi driver example for gpu compute minhashes
aws batch submit-job --job-name gpu_compute_minhashes-nemo\
                     --job-definition compute_minhashes-s3-csi-job \
                     --job-queue EKS-Batch-JQ1-nemo \
                     --region us-west-2

aws batch submit-job --job-name compute-minhashes-script-nemo \
                     --job-definition compute-minhashes-s3-read_parquet-job:2 \
                     --job-queue EKS-Batch-JQ1-nemo \
                     --region us-west-2

aws batch submit-job --job-name minhash-lsh-script-nemo \
                     --job-definition minhash-lsh-s3-read-parquet-job \
                     --job-queue EKS-Batch-JQ1-nemo \
                     --region us-west-2