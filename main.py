import os
import re

from flask import Flask, request
from google.cloud import bigquery


app = Flask(__name__)


@app.route("/", methods=["POST"])
def entry():
    # Load the file into BigQuery
    client = bigquery.Client()
    uri = "gs://cloud-run-bq-celine/sample-bq-load.csv"
    table = "fivetran-cf-api.cloud_run_bq.cloud-run-test"

    # If the user has not set the environment variable for the table then error out
    #if table is None:
    #    print("Error: Table env variable not set, so returning.")
    #    return ("Error BIGQUERY_TABLE environment variable is not set.", 500)

    # Setup the job to append to the table if it already exists and to autodetect the schema
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
    )

    # Run the load job
    load_job = client.load_table_from_uri(uri, table, job_config=job_config)

    # Run the job synchronously and wait for it to complete
    load_job.result()

    return (f"Loaded file located at {uri} into BQ table {table}", 200)


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
