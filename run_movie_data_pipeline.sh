#!/bin/bash

export GOOGLE_APPLICATION_CREDENTIALS="credentials.json"
export GOOGLE_CLOUD_PROJECT="thermal-formula-416221"
export DATAPROC_CLUSTER_NAME="cluster-moviezy969"
export DATAPROC_REGION="us-central1"


echo "Preparing data..."
if ! python3 src/prepare_data.py; then
    echo "Failed to execute prepare_data.py. Aborting."
    exit 1
fi


echo "Running data processing job..."
if ! python3 src/run_dataproc_job.py; then
    echo "Failed to execute run_dataproc_job.py. Aborting."
    exit 1
fi

echo "Data pipeline executed successfully."
