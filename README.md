# Airflow ETL Pipeline with Postgres and NASA API Integration

This project implements an **ETL (Extract, Transform, Load)** pipeline using **Apache Airflow**. The pipeline extracts data from an external API (NASA's *Astronomy Picture of the Day* — APOD), transforms the response into the fields we care about, and loads it into a **Postgres** database. The entire workflow is orchestrated, scheduled, and monitored by Airflow.

The project uses **Docker** (via the **Astro CLI**) to run Airflow and Postgres as isolated, reproducible services, and uses Airflow **hooks** and **operators** to handle the ETL steps.

---

## Architecture

The pipeline is defined as an Airflow **DAG** (Directed Acyclic Graph) that runs on a daily schedule:

`create_table` → `extract_apod` → `transform_apod_data` → `load_apod_data`

![Pipeline DAG Graph](images/Graph%20pipeline.png)

---

## Tech Stack

| Component | Purpose |
|---|---|
| **Apache Airflow** | Orchestrates, schedules, and monitors the ETL workflow |
| **Astro CLI** | Runs Airflow + Postgres locally in Docker |
| **PostgreSQL** | Stores the extracted and transformed data |
| **NASA APOD API** | External data source |
| **DBeaver** | Inspect/query the data in Postgres |

---

## Key Components

### Airflow for Orchestration
Airflow defines, schedules, and monitors the entire ETL pipeline. The DAG manages task dependencies so the steps run sequentially and reliably, with built-in retries to handle a flaky API.

### Postgres Database
A PostgreSQL database stores the extracted and transformed data. Postgres runs in a Docker container with data persisted through Docker volumes. We interact with it using Airflow's **`PostgresHook`**.

### NASA API (Astronomy Picture of the Day)
NASA's APOD API provides metadata about the daily astronomy picture — including the `title`, `explanation`, `url`, `date`, and `media_type`. We use Airflow's **`HttpOperator`** to extract data from the API.

---

## Workflow Stages

### 1. Extract (E)
The **`HttpOperator`** makes an HTTP GET request to NASA's APOD API. The response is JSON containing fields like the picture's title, explanation, and image URL.

### 2. Transform (T)
The extracted JSON is processed in the transform task using Airflow's **TaskFlow API** (the `@task` decorator). This stage picks the relevant fields — `title`, `explanation`, `url`, `date`, and `media_type` — and shapes them for the database.

### 3. Load (L)
The transformed data is loaded into a Postgres table using **`PostgresHook`**. If the target table (`apod_data`) doesn't exist, it is created automatically by a `create_table` task at the start of the DAG.

---

## Project Structure

```
ETLpipeline/
├── dags/
│   └── etl.py              # The ETL DAG definition
├── docker-compose.yml      # Postgres service definition
├── Dockerfile              # Astro/Airflow image
├── requirements.txt        # Python dependencies
├── images/                 # Screenshots used in this README
└── README.md
```

---

## Getting Started

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli)

### Run locally
```bash
# Start Airflow + Postgres in Docker
astro dev start
```
The Airflow UI will be available at **http://localhost:8080** (default login `admin` / `admin`).

### Configure Airflow Connections
In the Airflow UI, go to **Admin → Connections** and create:

**1. NASA API (`nasa_api`)**
- Connection Type: `HTTP`
- Host: `https://api.nasa.gov`
- In **Extra**: `{"api_key": "YOUR_NASA_API_KEY"}`

**2. Postgres (`my_postgres_connection`)**
- Connection Type: `Postgres`
- Host: `postgres`
- Schema/Database: `postgres`
- Login: `postgres`
- Password: `postgres`
- Port: `5432`

![Airflow Connections](images/connection_postgres%20and%20nasa_api.png)

### Trigger the DAG
In the Airflow UI, unpause and trigger the **`nasa_apod_postgres`** DAG. A successful run shows all tasks in green:

![Successful DAG Run](images/ETL_pipelinr%20test.png)

---

## Viewing the Data

Connect to Postgres from your host machine with DBeaver (or any SQL client):

| Field | Value |
|---|---|
| Host | `localhost` |
| Port | `10809` *(host-mapped port — check your Docker port mapping)* |
| Database | `postgres` |
| Username | `postgres` |
| Password | `postgres` |

Then query the loaded data:
```sql
SELECT * FROM apod_data;
```

The `url` field in each row links to that day's astronomy image — for example, the keogram fetched by the pipeline:

![APOD result fetched by the pipeline](images/Keogram.png)
