## Dibimbing Airflow Sandbox

A minimal, Dockerized Apache Airflow environment for local development and teaching. It ships with:

- Airflow 2.9.1 (Python 3.9) container
- Bind mounts for `dags/`, `scripts/`, and `data/`
- A sample DAG (`dags/sample_dag.py`) to demonstrate PythonOperator usage
- An entrypoint that initializes Airflow

This project is intended for local use. Do not use this configuration as-is in production.

---

### Repository structure

```
./
├─ dags/
│  └─ sample_dag.py
├─ docker/
│  ├─ Dockerfile.airflow
│  └─ docker-compose.yml
├─ scripts/
│  └─ airflow_entrypoint.sh
├─ requirements.txt
├─ makefile
└─ readme.md
```

---

### Prerequisites

- Docker (tested with Docker Desktop on macOS)
- Docker Compose v2 (`docker compose`)
- `make` (optional but recommended)

---

### Quickstart

1. Create a `.env` file at the repo root:

```env
# Container identity and webserver port
AIRFLOW_CONTAINER_NAME=airflow-local
AIRFLOW_HOST_NAME=airflow
AIRFLOW_WEBSERVER_PORT=8080
```

2. Build the Airflow image:

```bash
make build
```

3. Start the stack:

```bash
make spinup
```

This runs `docker compose` in the foreground. Access the Airflow web UI at:

- http://localhost:8080 (or the port you set via `AIRFLOW_WEBSERVER_PORT`)

To stop, press Ctrl+C in the terminal where it’s running, or in another terminal:

```bash
docker compose -f ./docker/docker-compose.yml down
```

If you prefer detached mode, run manually:

```bash
docker compose -f ./docker/docker-compose.yml --env-file .env up -d
```

---

### How it works

- The image is based on `apache/airflow:2.9.1-python3.9`.
- At runtime, the following host folders are mounted into the container:
  - `dags/` → `/opt/airflow/dags`
  - `scripts/` → `/scripts`
  - `data/` → `/opt/airflow/data` (create this folder locally if you need it)
- The container entrypoint (`/scripts/airflow_entrypoint.sh`) will:
  - Initialize the Airflow DB (`airflow db init`)
  - Set `AUTH_ROLE_PUBLIC = 'Admin'` in `webserver_config.py` (for local-only use)
  - Start the scheduler (background) and the webserver

---

### Sample DAG

File: `dags/sample_dag.py`

- DAG ID: `generate_and_ingest_activity_data`
- Schedule: `@once`
- Tasks: two `PythonOperator` tasks that call `generate_random_data()`

Important notes:

- The sample imports `numpy` and `pandas`. Only `pandas==2.1.0` is pinned in `requirements.txt`. Add `numpy` to `requirements.txt` and rebuild to avoid `ModuleNotFoundError`.
- The two tasks currently share the same `task_id` (`generate_random_data`). Airflow requires unique task IDs; rename them (e.g., `generate_random_data_1`, `generate_random_data_2`).

---

### Managing dependencies

Add Python packages to `requirements.txt`. Example:

```txt
pandas==2.1.0
numpy==1.26.4
```

Then rebuild the image:

```bash
make build
```

And restart the stack:

```bash
docker compose -f ./docker/docker-compose.yml --env-file .env up
```

---

### Make targets

```bash
make help     # Show available targets
make build    # Build the Airflow image (tag: dataeng-dibimbing/airflow)
make spinup   # Run docker compose with the provided .env (foreground)
```

Note: The `clean` target references `scripts/goodnight.sh`, which is not present. Use `docker compose down` instead.

---

### Known issues and fixes

1. Docker build fails with: `Could not open requirements file: [Errno 2] No such file or directory: '/opt/airflow/requirements.txt'`

The Dockerfile expects `/opt/airflow/requirements.txt` but does not copy it. Update `docker/Dockerfile.airflow` to include:

```dockerfile
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install -r /opt/airflow/requirements.txt
```

2. `ModuleNotFoundError: No module named 'numpy'`

Add `numpy` to `requirements.txt` and rebuild the image.

3. Duplicate task_id in `sample_dag.py`

Rename the second task’s `task_id` (e.g., `generate_random_data_2`).

4. Airflow UI auth is wide open

`AUTH_ROLE_PUBLIC = 'Admin'` is set for convenience in local dev. Do not use this setting outside local environments.

5. Entrypoint execution permissions

Ensure the script is executable:

```bash
chmod +x scripts/airflow_entrypoint.sh
```

---

### Troubleshooting

- Port already in use: change `AIRFLOW_WEBSERVER_PORT` in `.env`.
- Compose fails on first run: remove dangling containers and retry
  ```bash
  docker compose -f ./docker/docker-compose.yml down -v
  docker compose -f ./docker/docker-compose.yml --env-file .env up --build
  ```
- Logs: view container logs
  ```bash
  docker logs -f "$AIRFLOW_CONTAINER_NAME"
  ```
- Shell into the container
  ```bash
  docker exec -it "$AIRFLOW_CONTAINER_NAME" bash
  ```

---

### Roadmap (suggested)

- Add database and BI services to `docker-compose.yml`
- Move to a production-safe auth model for Airflow UI
- Parameterize Airflow user creation on first run

---

### Security note

This configuration disables authentication for convenience. Use only on a private, local machine and never expose it to the internet.

---

### License

MIT or your preferred license.
