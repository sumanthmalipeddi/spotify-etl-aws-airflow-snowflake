# Spotify ETL Pipeline — AWS & Airflow

A data engineering project that extracts Spotify playlist data via the Spotify API, transforms it, and loads structured CSVs into AWS S3 — built in two phases to demonstrate progression from a serverless architecture to a fully orchestrated pipeline.

---

## Architecture

### Phase 1 — Serverless (AWS Lambda + CloudWatch)
![Architecture](https://github.com/sumanthmalipeddi/spotify-etl-aws-airflow/blob/main/Architecture.png)

### Phase 2 — Orchestrated (Apache Airflow + Docker)
- Airflow DAG replaces Lambda + CloudWatch triggers
- Docker Compose runs the full Airflow stack locally
- Same S3 buckets used for raw and transformed data storage

---

## Project Evolution

### Phase 1 — AWS Lambda + CloudWatch (Serverless)
The first version of the pipeline was built entirely on AWS managed services:
- **CloudWatch** triggered the pipeline on a schedule
- **Lambda** extracted data from the Spotify API and stored raw JSON in S3
- A second **Lambda** transformed the data and saved CSVs to S3
- **Glue Crawler** catalogued the transformed data
- **Athena** was used for SQL queries on top of the S3 data

### Phase 2 — Apache Airflow + Docker (Orchestrated)
Rebuilt the pipeline using Apache Airflow to gain full visibility and control over task orchestration:
- **Airflow DAG** with 9 tasks replaces both Lambda functions and the CloudWatch trigger
- **Docker Compose** runs the complete Airflow environment locally
- Tasks: `fetch_spotify_data` → `upload_raw_to_s3` → `read_data_from_s3` → `process_album / process_artist / process_songs` → `store_album / store_artist / store_songs` → `move_processed_data`
- Airflow **Variables** store Spotify credentials securely
- Airflow **Connections** manage AWS credentials via `S3Hook`

---

## Tech Stack

| Layer | Phase 1 | Phase 2 |
|---|---|---|
| Orchestration | AWS CloudWatch | Apache Airflow |
| Compute | AWS Lambda | Docker (local) |
| Storage | AWS S3 | AWS S3 |
| Cataloguing | AWS Glue Crawler | — |
| Querying | AWS Athena | — |
| Language | Python | Python |
| Key Libraries | boto3, spotipy, pandas | spotipy, pandas, apache-airflow, airflow-providers-amazon |

---

## Repository Structure

```
spotify-etl-aws-airflow/
├── Architecture.png                          # Phase 1 architecture diagram
├── Spotify Data Pipeline Project.ipynb       # Phase 1 exploration notebook
├── spotify_api_data_extract.py               # Phase 1 Lambda extract function
├── spotify_transformation_load_function.py   # Phase 1 Lambda transform function
└── airflow_pipeline/                         # Phase 2 — Airflow orchestration
    ├── Dockerfile
    ├── docker-compose.yaml
    ├── requirements.txt
    └── dags/
        └── spotify_etl_pipeline.py           # Full 9-task Airflow DAG
```

---

## Phase 1 — Setup

```bash
pip install spotipy pandas
```

Set environment variables for Lambda:
```
client_id = <your_spotify_client_id>
client_secret = <your_spotify_client_secret>
```

> Never expose your client ID and secret — always use environment variables or AWS Secrets Manager.

---

## Phase 2 — Airflow Setup

**Prerequisites:** Docker and Docker Compose installed.

```bash
cd airflow_pipeline

# Start Airflow
docker-compose up -d

# Access UI at http://localhost:8080
# Default credentials: airflow / airflow
```

**Airflow Variables to set (Admin → Variables):**

| Key | Value |
|---|---|
| `spotify_client_id` | Your Spotify client ID |
| `spotify_client_secret` | Your Spotify client secret |

**Airflow Connection to set (Admin → Connections):**
- Conn ID: `aws_s3_airbnb`
- Conn Type: `Amazon Web Services`
- Login: AWS Access Key ID
- Password: AWS Secret Access Key

---

## DAG Overview

```
fetch_spotify_data
      |
upload_raw_to_s3
      |
read_data_from_s3
      |
  --------------------------------
  |              |               |
process_album  process_artist  process_songs
  |              |               |
store_album    store_artist    store_songs
  --------------------------------
      |
move_processed_data
```

---

## Data Source

Spotify API via [spotipy](https://spotipy.readthedocs.io/)

The pipeline extracts track, album, and artist details from any public Spotify playlist — configured via the playlist URL in the DAG.

---

## Phase 1 — Data Load Results (Athena)

After Glue Crawler catalogued the S3 data into `spotify_db`:

```sql
SELECT * FROM "spotify_db"."songs" LIMIT 10;
```
![Songs Data](https://github.com/sumanthmalipeddi/spotify_trending_telugu/assets/118842072/d62cfef6-e30d-4c9a-b52d-ef5464353443)

```sql
SELECT * FROM "spotify_db"."albums" LIMIT 10;
```
![Album Data](https://github.com/sumanthmalipeddi/spotify_trending_telugu/assets/118842072/0acf8dba-935f-402a-9205-fe8847fd36e8)

```sql
SELECT * FROM "spotify_db"."artists" LIMIT 10;
```
![Artist Data](https://github.com/sumanthmalipeddi/spotify_trending_telugu/assets/118842072/c1e7de80-dcc3-44c9-8093-23127dc2fd1c)
