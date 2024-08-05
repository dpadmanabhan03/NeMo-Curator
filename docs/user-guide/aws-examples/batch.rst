===================
NeMo on AWS Batch
====================

Setup AWS Batch:
----------------

    kubectl apply -f batch/eks-batch-setup/namespace.yaml

    kubectl apply -f batch/eks-batch-setup/clusterrole.yaml

    kubectl apply -f batch/eks-batch-setup/compute-env-role.yaml

    eksctl create iamidentitymapping \
        --cluster $cluster-name \
        --arn "arn:aws:iam:::role/AWSServiceRoleForBatch" \
        --username aws-batch

Create compute environment
--------------------------

    aws batch create-compute-environment --cli-input-json file://batch/eks-batch-setup/compute-environment.json

Create job queue
-----------------

    aws batch create-job-queue --cli-input-json file://batch/eks-batch-setup/job-queue.json


Register a job definition
-------------------------

  Build a custom image using the Dockerfile in batch dir.

  Kubeclient.py execs into the dask scheduler pod and runs nemo curator commands. The scheduler pod distributes job to the workers.

  Necessary permission for a pod to exec into another pod is provided in cluster-role.yaml.

    aws batch register-job-definition --cli-input-json file://batch/batch-job-definition/job-definition.json

## Submit job
-------------

    aws batch submit-job --job-name compute-minhashes-script-nemo \
                     --job-definition compute-minhashes-s3-read_parquet-job:2 \
                     --job-queue EKS-Batch-JQ1-nemo \
                     --region us-west-2

