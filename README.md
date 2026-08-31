# MPROP Data Ingestion Listener

This service automates MPROP property data ingestion into Google BigQuery.

When a CSV file is uploaded to a Google Cloud Storage bucket, an Eventarc event invokes this Cloud Run service. The service uses the uploaded filename to determine the destination table, then loads the CSV data into the `mke_rag_demo` BigQuery dataset using the schema already defined on that table.

# Related Repos

Terraform
https://github.com/bkelly0/gcp-mke-rag-terraform

CSV Ingestion Service
https://github.com/bkelly0/mke-property-rag-csv-listener

PDF Vector Embedding Ingestion Service
https://github.com/bkelly0/mke-demo-rag-event-handler

Property Geocoding Service
https://github.com/bkelly0/mke-property-rag-geocode

## Ingestion flow

1. A CSV file is uploaded to Google Cloud Storage.
2. Eventarc sends the finalized-object event to the Cloud Run service.
3. The service derives the BigQuery table name from the CSV filename.
4. BigQuery loads the CSV into `mke_rag_demo`, using the target table's schema.
5. The existing table data is replaced by the newly uploaded data.
6. After `mprop_master.csv` is loaded, the service publishes a notification to Pub/Sub for downstream RAG processing.

For example:

```text
gs://bucket-name/mprop_master.csv
    -> mke_rag_demo.mprop_master
```

## Endpoints

- `GET /` returns a health-check response.
- `POST /` handles Cloud Storage finalization events.

## Configuration

- `PROJECT_ID`: Google Cloud project ID. If omitted, the project is read from the BigQuery client.
- `TOPIC_ID`: Pub/Sub topic used after `mprop_master.csv` ingestion. Defaults to `mke-rag-data-ingested`.

## Development

Install dependencies:

```bash
pip install -r requirements.txt
```

Run the tests:

```bash
python -m pytest -q tests/test_main.py
```

## Deployment

Cloud Build builds the Docker image, pushes it to Artifact Registry, and deploys the service to Cloud Run. The build and deployment configuration is defined in `cloudbuild.yaml`.
