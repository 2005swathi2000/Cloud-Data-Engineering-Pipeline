from google.cloud import storage
from google.cloud import bigquery
import os

# Function to get the list of new files
def get_new_files(project_id, bucket_name, dataset, parquet_path):
    # Set up GCP storage client
    client = storage.Client(project = project_id)
    bucket = client.get_bucket(bucket_name.replace('gs://', ''))

    # Get all parquet files in the folder
    blobs = bucket.list_blobs(prefix=parquet_path + '/')

    # Get the filenames of the processed files from BigQuery table
    client = bigquery.Client(project = project_id)
    query = f"""
    SELECT 
    archivo
    FROM `{project_id}.{dataset}.registro`
    ORDER BY fecha DESC
    LIMIT 100
    """
    lista_procesados = client.query(query, location='US').to_dataframe()['archivo'].tolist()
    
    processed_files = set(lista_procesados)

    # Get new files that haven't been processed yet
    new_files = [blob.name for blob in blobs if blob.name not in processed_files and blob.name.endswith(".parquet")]

    return new_files

# Update the list of processed files
def update_processed_files(file_name, project_id, dataset):

    client = bigquery.Client(project = project_id)

    query = f"""
    INSERT INTO `{project_id}.{dataset}.registro` (archivo, fecha)
    VALUES ('{file_name}', DATETIME(CURRENT_TIMESTAMP()))
    """

    client.query(query, location='US')