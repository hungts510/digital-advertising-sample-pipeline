# Peer39 Senior-DE exercise 2025
# PySpark + MinIO + Airflow - Local Data Pipeline (Docker Compose)

## Overview
This project provides a local data engineering stack using **PySpark**, **MinIO** (S3-compatible object storage), **Airflow** (orchestration), and **Docker Compose**. It is designed for rapid prototyping and local development of Spark jobs that interact with S3 storage and can be orchestrated with Airflow.

---

## Prerequisites
- [Docker](https://www.docker.com/get-started) and [Docker Compose](https://docs.docker.com/compose/)
- (Optional) [Python 3.8+](https://www.python.org/) if you want to run/test PySpark jobs locally

---

## Quick Start: Clean and Create the Stack

### 1. **Clone the repository**
```sh
git clone <your-repo-url>
cd <your-repo-directory>
```

### 2. **Clean the stack (recommended for a fresh start)**
This will remove all containers, images, volumes, and networks related to this project, ensuring a clean environment:
```sh
./clean-stack.sh
```
- **Note:** This is safe to run multiple times and will not affect other Docker projects on your system.

### 3. **Create and start the stack**
This will build and launch all services:
```sh
./create-stack.sh
```
- This will:
  - Start MinIO, Spark (master/worker), Airflow (webserver/scheduler), and Postgres
  - Upload the sample CSV to MinIO
  - Download all required Spark/Hadoop/AWS JARs for S3A support
  - Set up all configs and volumes
  - Create a default Airflow admin user

### ⚠️ Note on Airflow Startup Time

After running `./create-stack.sh`, it may take **3–5 minutes** for the Airflow web UI to become accessible at [http://localhost:8081](http://localhost:8081). This is normal, especially after a full clean or on the first run, as Airflow needs to initialize its database and install dependencies.

- You can check progress with:
  ```sh
  docker compose logs -f airflow-webserver
  ```
- The UI will be available once you see a message like:
  `Listening at: http://0.0.0.0:8080`

### 4. **Access the services**
- **MinIO UI:** [http://localhost:9001](http://localhost:9001)
  - **Username:** `minioadmin`
  - **Password:** `minioadmin`
- **Spark Master UI:** [http://localhost:8080](http://localhost:8080)
- **Airflow UI:** [http://localhost:8081](http://localhost:8081)
  - **Username:** `admin`
  - **Password:** `admin`
- The sample data should be in the `sample-bucket` bucket in MinIO.

### 5. **Run a PySpark job**
- Example: Read a CSV from S3 and print its schema/rows
- From inside the spark-master container:
  ```sh
  docker exec -it spark-master /bin/bash
  spark-submit --master spark://spark-master:7077 /opt/bitnami/spark/jobs/read_s3_csv.py
  ```
- Or trigger via Airflow (see the DAG in `airflow/dags/`).

### 6. **Add your own jobs**
- Place new PySpark scripts in `spark/jobs/`.
- Use the S3A path format: `s3a://sample-bucket/yourfile.csv`
- All S3A configs are handled in `spark/conf/spark-defaults.conf` and `core-site.xml`.

---

## Cleaning the Stack (Advanced)
If you ever want to fully reset your environment (for example, before testing changes or to ensure reproducibility), run:
```sh
./clean-stack.sh
```
This will:
- Stop and remove all containers, networks, and volumes defined in this project's `docker-compose.yml`
- Remove all related Docker images
- Remove any dangling images built by Compose
- Remove only resources related to this project (safe for your other Docker projects)

---

## Troubleshooting
- **Dependency errors (ClassNotFoundException, NoClassDefFoundError):**
  - Make sure all required Hadoop and AWS SDK v2 JARs are present in `/opt/bitnami/spark/jars/` in both master and worker containers.
  - See the `spark-init.sh` logic (if present) or manually download missing JARs as described in the docs.
- **MinIO/S3 errors:**
  - Ensure MinIO is running and the sample data is uploaded to the correct bucket.
- **Airflow issues:**
  - Check the Airflow logs (`docker compose logs airflow-webserver`) and ensure all dependencies are installed.
  - The default Airflow login is `admin` / `admin`.
- **Permissions:**
  - If you mount local directories, ensure they are writable by the container user (UID 1001 or root).

---

## Notes
- The stack is for local development and prototyping only.
- All configuration is handled via Docker Compose and the `create-stack.sh` script.
- You can extend the stack with more jobs, DAGs, or data as needed.
- The `clean-stack.sh` script is a safe way to reset your environment for repeatable results.

---

## Credits
- Spark: https://spark.apache.org/
- MinIO: https://min.io/
- Airflow: https://airflow.apache.org/
- Bitnami Docker images 