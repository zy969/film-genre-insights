import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define project, region, and other details
PROJECT_ID = "thermal-formula-416221"
REGION = "us-central1"
CLUSTER_NAME = "cluster-moviezy969"
GCS_BUCKET = "movie-zy969"
SPARK_FILE_NAME = "spark.py"
LOCAL_SPARK_FILE_PATH = os.path.join("src", SPARK_FILE_NAME)
GCS_SPARK_FILE_PATH = f"gs://{GCS_BUCKET}/{SPARK_FILE_NAME}"

def upload_file_to_gcs():
    """Uploads local Spark script to GCS."""
    try:
        command = f"gsutil cp {LOCAL_SPARK_FILE_PATH} {GCS_SPARK_FILE_PATH}"
        os.system(command)
        logger.info("Local Spark script uploaded to GCS.")
    except Exception as e:
        logger.error(f"Failed to upload local Spark script to GCS: {e}")
        raise

def check_cluster_exists(cluster_name):
    """Checks if the specified Dataproc cluster exists."""
    try:
        command = f"gcloud dataproc clusters describe {cluster_name} --region {REGION} --format=json"
        result = os.popen(command).read()
        return "state" in result
    except Exception as e:
        logger.error(f"Failed to check if Dataproc cluster exists: {e}")
        raise

def create_dataproc_cluster():
    """Creates a Dataproc single-node cluster if it doesn't exist."""
    try:
        if not check_cluster_exists(CLUSTER_NAME):
            command = f"gcloud dataproc clusters create {CLUSTER_NAME} " \
                      f"--region {REGION} " \
                      "--single-node " \
                      "--master-machine-type n1-standard-1 " \
                      "--master-boot-disk-size 20 " \
                      "--worker-boot-disk-size 50 " \
                      "--image-version 1.5-debian " \
                      "--initialization-actions gs://dataproc-initialization-actions/python/pip-install.sh " \
                      "--metadata 'PIP_PACKAGES=google-cloud-storage' " \
                      "--optional-components=ANACONDA " \
                      "--enable-component-gateway"
            os.system(command)
            logger.info("Dataproc single-node cluster created.")
        else:
            logger.info(f"Cluster {CLUSTER_NAME} already exists.")
    except Exception as e:
        logger.error(f"Failed to create Dataproc cluster: {e}")
        raise

def submit_spark_job():
    """Submits a Spark job to the Dataproc cluster."""
    try:
        command = f"gcloud dataproc jobs submit pyspark " \
                  f"--cluster {CLUSTER_NAME} " \
                  f"--jars gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar " \
                  f"--driver-log-levels root=FATAL {GCS_SPARK_FILE_PATH}"
        os.system(command)
        logger.info("Spark job submitted to the Dataproc cluster.")
    except Exception as e:
        logger.error(f"Failed to submit Spark job to Dataproc cluster: {e}")
        raise

if __name__ == "__main__":
    try:
        os.system(f"gcloud config set project {PROJECT_ID}")
        os.system(f"gcloud config set dataproc/region {REGION}")

        upload_file_to_gcs()
        create_dataproc_cluster()
        submit_spark_job()
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
