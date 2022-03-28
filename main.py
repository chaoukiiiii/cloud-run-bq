import os
import re

from flask import Flask, request
from google.cloud import bigquery


app = Flask(__name__)


@app.route("/", methods=["POST","GET"])
def entry():
    # Load the file into BigQuery
    client = bigquery.Client()
    bucket = os.environ.get('BUCKET')
    folder=os.environ.get('FOLDER')
    pattern=os.environ.get('PATTERN')
    delimiter=os.environ.get('DELIMITER')
    dataset=os.environ.get('DATASET')
    table_name=os.environ.get('TABLENAME')
    archive_folder=os.environ.get('ARCHIVEFOLDER')
    #bucket = "gs://cloud-run-bq-celine"
    #folder = "covid_folder"
    #pattern="example_data_covid"
    #delimter=","
    #dataset="cloud_run_bq"
    #table_name = "cloud_run_bq_init"
    #archive_folder="covid_folder_archive"
    print ("display Ingestion Configuration")
    print("bucket Name :", bucket)
    print("folder Name :", folder)
    print("pattern of files :", pattern)
    print("delimter  :", delimter)
    print("dataset Name :", dataset)
    print("table Name :",table_name)
    print("archive_folder Name :",archive_folder)
    table=dataset+"."+table_name
    # If the user has not set the environment variable for the table then error out
    #if table is None:
    #    print("Error: Table env variable not set, so returning.")
    #    return ("Error BIGQUERY_TABLE environment variable is not set.", 500)
    uri=bucket+"/"+folder+"/"+pattern+"*.csv"
    # Setup the job to append to the table if it already exists and to autodetect the schema
    job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=True
    )

    # Run the load job
    load_job = client.load_table_from_uri(uri, table, job_config=job_config)

    # Run the job synchronously and wait for it to complete
    load_job.result()

    print ("Loaded file located at")
    storage_client = storage.Client()
    bucket="cloud-run-bq-celine"
    bucket_initial = storage_client.get_bucket(bucket)
    blobs = bucket_initial.list_blobs(prefix=folder+'/'+pattern)
    for i in blobs:
        bucket_initial.rename_blob(i, new_name=i.name.replace(folder+'/', archive_folder+'/archived'))


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
