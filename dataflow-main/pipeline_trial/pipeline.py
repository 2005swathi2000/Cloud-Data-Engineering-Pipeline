# General imports
import time
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import WriteToBigQuery
from apache_beam.runners import DirectRunner#, DataflowRunner
from apache_beam.io.gcp.bigquery import bigquery_tools
import argparse
import logging
import json
import subprocess

## Local imports
from pipeline_trial.custom_fns import etl
from pipeline_trial.custom_fns import utils

# Define the pipeline
def run(argv=None, save_main_session=False):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--project',
        required=True,
        dest = 'project',
        help='The project id where magic happens.')
    parser.add_argument(
        '--bucket',
        required=True,
        dest = 'bucket',
        help='The bucket name where the files are stored.')
    parser.add_argument(
        '--parquetpath',
        required=True,
        dest = 'parquetpath',
        help='The file path for the new parquet files.')
    parser.add_argument(
        '--dataset',
        required=True,
        dest='dataset',
        help='The BigQuery dataset where data is gonna be stored.')
    parser.add_argument(
        '--table',
        required=True,
        dest='table',
        help='The BigQuery table to store the data.')
    
    args, beam_args = parser.parse_known_args(argv)

    try:
        # bq show --format=prettyjson PROJECT_ID:DATASET_NAME.TABLE_NAME > schema_downloaded.json
        subprocess.run(["bq", "show", "--format=prettyjson", f"{args.project}:{args.dataset}.{args.table}"], stdout=open("./schema_downloaded.json", "w"))
        table_schema = bigquery_tools.parse_table_schema_from_json(json.dumps(json.load(open("./schema_downloaded.json"))["schema"]))
    except:
        table_schema = bigquery_tools.parse_table_schema_from_json(json.dumps(json.load(open("./schema_original.json"))["schema"]))

    beam_options = PipelineOptions(beam_args, save_main_session=save_main_session)

    full_file_path = args.bucket + '/'

    while True:
        new_files = utils.get_new_files(args.project, args.bucket, args.dataset, args.parquetpath)

        if len(new_files) == 0:
            print(f"No new files found. Exiting pipeline")
            break
        else:
            print('Total files founded: ' + str(len(new_files)))
            print('Starting pipeline...')
            for file_name in new_files:
                print(f"Processing new file: {file_name}")

                with beam.Pipeline(runner=DirectRunner(), options=beam_options) as p:
                    (
                        p
                        | 'Reading the Parquetfile' >> beam.io.ReadFromParquetBatched(full_file_path + file_name, columns=['business_id', 'attributes'])
                        | 'Converting to Pandas' >> beam.Map(lambda table: table.to_pandas())
                        | 'Cleaning and transforming data' >> beam.FlatMap(etl.clean_attributes)
                        | 'Uploading to BigQuery' >> WriteToBigQuery(
                            table=args.table,
                            dataset=args.dataset,
                            project=args.project,
                            schema=table_schema,
                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                            batch_size=int(100)
                        )
                    )

                print('Done processing file: ' + file_name)
                utils.update_processed_files(file_name, args.project, args.dataset)

            print(f'Done processing all files. Checking for new files and exiting if no new files are founded')

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.WARNING)
    run()