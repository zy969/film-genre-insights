import os
import logging
import pandas as pd
from google.api_core.exceptions import NotFound
from prefect_gcp import GcpCredentials, GcsBucket
from google.cloud import bigquery
from io import StringIO
from prefect import flow, task

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set credentials and initialize GCS bucket
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(os.getcwd(), "credentials.json")
gcp_creds = GcpCredentials()
gcs_bucket = GcsBucket(bucket="movie-zy969", gcp_credentials=gcp_creds)

CSV_FILE_NAME = "TMDB_movie_dataset_v11.csv"
os.environ["KAGGLE_CONFIG_DIR"] = os.path.join(os.getcwd(), "kaggle")
import kaggle

@task
def setup_bigquery(schema):
    """Create/update BigQuery dataset and table."""
    client = bigquery.Client()
    dataset_id = 'movie'
    dataset_ref = client.dataset(dataset_id)
    table_id = "movie"
    table_ref = dataset_ref.table(table_id)

    try:
        client.get_dataset(dataset_ref)
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        logger.info(f"Dataset {dataset_id} created.")

    try:
        client.get_table(table_ref)
    except NotFound:
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        logger.info(f"Table {table_id} created or updated.")

    return True

@task
def fetch_data():
    """Download dataset from Kaggle."""
    dataset_path = "asaniczka/tmdb-movies-dataset-2023-930k-movies"
    try:
        kaggle.api.dataset_download_files(dataset_path, path='.', unzip=True)
    except Exception as e:
        logger.error(f"Failed to download dataset from Kaggle: {e}")
        raise
    file_name = CSV_FILE_NAME
    with open(file_name, 'r') as file:
        content = file.read()
    return content

@task
def transform_data(content):
    """Process and convert data types."""
    df = pd.read_csv(StringIO(content))
    conversions = {
        'id': ('coerce', int),
        'vote_count': ('coerce', int),
        'revenue': ('coerce', int),
        'runtime': ('coerce', int),
        'vote_average': ('coerce', float),
        'popularity': ('coerce', float),
        'adult': (None, bool),
        'release_date': ('coerce', 'datetime64[ns]')
    }
    for col, (action, dtype) in conversions.items():
        if dtype in [int, float]:
            df[col] = pd.to_numeric(df[col], errors=action).fillna(0).astype(dtype)
        elif dtype is bool:
            df[col] = df[col].map({'True': True, 'False': False, True: True, False: False}).astype(dtype)
        elif dtype == 'datetime64[ns]':
            df[col] = pd.to_datetime(df[col], errors=action).dt.date
    return df

@task
def to_parquet(df):
    """Convert DataFrame to Parquet format."""
    try:
        df.to_parquet("movie.parquet")
    except Exception as e:
        logger.error(f"Failed to convert DataFrame to Parquet format: {e}")
        raise
    return "movie.parquet"

@task
def gcs_upload(parquet_path):
    """Upload file to GCS."""
    gcs_path = "movie.parquet"
    try:
        bucket_path = gcs_bucket.upload_from_path(from_path=parquet_path, to_path=gcs_path)
    except Exception as e:
        logger.error(f"Failed to upload file to GCS: {e}")
        raise
    logger.info(f"File uploaded to GCS: {bucket_path}")
    return bucket_path

@task
def bigquery_upload(df):
    """Upload data to BigQuery."""
    client = bigquery.Client()
    table_id = "movie.movie"
    try:
        job = client.load_table_from_dataframe(df, table_id)
        job.result()
        logger.info(f"Uploaded {job.output_rows} rows to BigQuery table {table_id}")
    except Exception as e:
        logger.error(f"Failed to upload data to BigQuery: {e}")
        raise

@task
def delete_local_file(file_path):
    """Delete local file."""
    if os.path.exists(file_path):
        try:
            os.remove(file_path)
            logger.info(f"Deleted local file: {file_path}")
        except Exception as e:
            logger.error(f"Failed to delete local file: {e}")
            raise
    else:
        logger.warning(f"File not found: {file_path}")

@flow
def data_pipeline_flow():
    """Main data pipeline flow."""
    content = fetch_data()
    df = transform_data(content)
    parquet_path = to_parquet(df)
    gcs_path = gcs_upload(parquet_path)
    delete_local_file(parquet_path)

    schema = [
        bigquery.SchemaField('id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('title', 'STRING'),
        bigquery.SchemaField('vote_average', 'FLOAT'),
        bigquery.SchemaField('vote_count', 'INTEGER'),
        bigquery.SchemaField('status', 'STRING'),
        bigquery.SchemaField('release_date', 'DATE'),
        bigquery.SchemaField('revenue', 'INTEGER'),
        bigquery.SchemaField('runtime', 'INTEGER'),
        bigquery.SchemaField('adult', 'BOOLEAN'),
        bigquery.SchemaField('backdrop_path', 'STRING'),
        bigquery.SchemaField('budget', 'INTEGER'),
        bigquery.SchemaField('homepage', 'STRING'),
        bigquery.SchemaField('imdb_id', 'STRING'),
        bigquery.SchemaField('original_language', 'STRING'),
        bigquery.SchemaField('original_title', 'STRING'),
        bigquery.SchemaField('overview', 'STRING'),
        bigquery.SchemaField('popularity', 'FLOAT'),
        bigquery.SchemaField('poster_path', 'STRING'),
        bigquery.SchemaField('tagline', 'STRING'),
        bigquery.SchemaField('genres', 'STRING'),
        bigquery.SchemaField('production_companies', 'STRING'),
        bigquery.SchemaField('production_countries', 'STRING'),
        bigquery.SchemaField('spoken_languages', 'STRING')
    ]

    setup_bigquery(schema)
    bigquery_upload(df)
    delete_local_file(CSV_FILE_NAME)
    logger.info(f"Total rows in the dataset: {len(df)}")

data_pipeline_flow()
