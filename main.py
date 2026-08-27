import json
import os
import re
import logging
from typing import Optional
from google.cloud import bigquery
from fastapi import FastAPI, HTTPException, Request
from google.cloud.logging_v2.handlers import StructuredLogHandler
from google.cloud import pubsub_v1

PROJECT_ID = os.environ.get("PROJECT_ID", None)
TOPIC_ID = os.environ.get("TOPIC_ID", "mke-rag-data-ingested")


def get_project_id() -> str:
    if PROJECT_ID:
        return PROJECT_ID
    return get_bq_client().project

# Cache the client after the first ingestion request to reuse it across requests.
bq_client: Optional[bigquery.Client] = None
app = FastAPI()

def build_logger() -> logging.Logger:
    logger = logging.getLogger("rag-event-handler")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if not logger.handlers:
        # Structured JSON to stdout; Cloud Run/GCP ingests this automatically.
        handler = StructuredLogHandler()
        logger.addHandler(handler)

    return logger

logger = build_logger();

def get_bq_client() -> bigquery.Client:
    global bq_client
    if bq_client is None:
        bq_client = bigquery.Client()
    return bq_client


def load_gcs_file_into_bigquery(bucket: str, file_name: str) -> tuple[int, str]:
    client = get_bq_client()
    uri = f"gs://{bucket}/{file_name}"

    logger.info(f"Eventarc triggered by new file: {uri}")

    # use filename as table name
    raw_filename = file_name.split("/")[-1]
    table_name = re.sub(r"\.[^.]+$", "", raw_filename) 
    
    project_id = client.project
    dataset_id = "mke_rag_demo"
    table_id = f"{project_id}.{dataset_id}.{table_name}"

    try:
        # Fetch the live schema defined by Terraform
        logger.info(f"Fetching active infrastructure schema for target table: {table_id}")
        target_table = client.get_table(table_id)
        schema = target_table.schema

        # Configure Load Job using the fetched schema definitions
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            # Truncate existing data - to support adding data from a new year in the future, update to write to a temp table and merge into the existing table
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE_DATA,
            
            # Enforce the strict infrastructure schema fetched from BQ
            schema=schema,
            autodetect=False 
        )

        # 4. Execute Ingestion Job
        print(f"Appending data from {uri} into {table_id}...")
        load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
        load_job.result()  # Blocks until execution completes

        output_rows = load_job.output_rows
        logger.info(f"Successfully loaded {output_rows} rows into {table_id}!")
        return output_rows, table_name

    except Exception as error:
        logger.error(f"Error executing pipeline for {file_name}: {error}")
        raise

@app.get("/")
def health_check() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/")
async def gcs_to_bigquery_trigger(request: Request) -> dict[str, int | str]:
    # Handle an Eventarc GCS storage.objects.v1.finalized CloudEvent.
    try:
        data = await request.json()
        bucket = data["bucket"]
        file_name = data["name"]
    except (KeyError, TypeError, ValueError) as error:
        raise HTTPException(
            status_code=400,
            detail="Expected a CloudEvent body containing 'bucket' and 'name'.",
        ) from error

    output_rows, table_name = load_gcs_file_into_bigquery(bucket, file_name)

    if (table_name == 'mprop_master'):
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path(get_project_id(), TOPIC_ID)
        payload = {
            "bucket": bucket,
            "file_name": file_name,
            "table_name": table_name,
            "output_rows": output_rows,
        }
        future = publisher.publish(topic_path, data=json.dumps(payload).encode("utf-8"))
        message_id = future.result()
        logger.info(f"Published message {message_id} to {topic_path}")

    return {"status": "success", "output_rows": output_rows}
