# Parquet to BigQuery Dataflow Pipeline
This repository contains an Apache Beam batch processing pipeline that reads Parquet files from Google Cloud Storage (GCS), performs ETL transformations, and writes the processed data to a BigQuery table. The pipeline is designed to run on Google Cloud Dataflow. 
For example purposes, it uses a subset of the [Yelp dataset](https://www.yelp.com/dataset/documentation/main). The ETL transformations and full project can be found [here](https://github.com/HenryLABFinalGrupo02/trabajofinal).

## Overview
The pipeline is built using Apache Beam and includes the following main components:

1. Reading Parquet files from a GCS bucket.
2. Parsing the Parquet files and extracting the data.
3. Performing custom ETL transformations on the data.
4. Writing the processed data to a BigQuery table.
5. Monitoring for new files in the GCS bucket and processing them on arrival.

The pipeline uses custom functions located in the `pipeline_trial.custom_fns` module for ETL transformations and utility functions.

## Usage
1. Install the Google Cloud console. Create a Project on Google, GCS Bucket, BigQuery dataset, and activate necessary permissions for using Google Dataflow and Google Artifact Registry. 
2. Add a table named "registro" with the following schema: "archivo:STRING, fecha: DATETIME". 
3. Modify the `etl.py` script and add the necessary transformation in case you want to use your own data and ETL functions. If you use your own data, please modify "schema_original.json" to match your data. This will instruct BigQuery to create the table where data will be loaded. 
4. Modify the Makefile with your GCP project data.
5. Run `make init` just one time to create the necessary buckets and permissions. 
6. Run `make template` to create your custom template, which will be saved on Artifact Registry.
7. Go to Dataflow and add a job with a custom template selecting the `.json` saved in the new bucket. Remember to put all the parameters and in optional parameters also complete runtime, temporary, and staging location (usually gs://yourbucket/temp and /staging).
8. In case your script fails, check the log saved in /staging. 
9. Add `sample.snappy.parquet` to your bucket directory, which you specified and wait for the magic to happen. 
10. Enter BigQuery and make a query on your table and registro table to check your results. 

### TODO
- More detailed tutorial. 
- Proper script documentation. 

## Acknowledgments
Thanks to [kevenpinto](https://github.com/kevenpinto/beam_flex_demo) which provides sample code for streaming on Apache beamand a [Medium](https://medium.com/cts-technologies/building-and-testing-dataflow-flex-templates-80ef6d1887c6) tutorial. 