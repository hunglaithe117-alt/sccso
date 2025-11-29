# Mini SonarQube Scanner

This is a simplified tool to scan git repositories at specific commits using SonarQube. It includes a web interface for managing scans and viewing results.

## Prerequisites

-   **Docker** and **Docker Compose** (Recommended for easy setup)
-   **Python 3.12+** (If running locally without Docker)
-   **uv** (Optional, for local Python package management)

## Quick Start (Docker)

This is the easiest way to get everything running (MongoDB, SonarQube, and the Scanner API).

1.  **Configure Environment:**
    Open `docker-compose.yml` and update the `GITHUB_TOKENS` environment variable in the `scan-api` service. This is required for replaying missing commits (forks).
    ```yaml
    environment:
      - GITHUB_TOKENS=your_github_token_here
    ```

2.  **Start Services:**
    ```bash
    docker-compose up --build -d
    ```

3.  **Access Interfaces:**
    -   **Web Interface (Swagger UI):** [http://localhost:8000/docs](http://localhost:8000/docs)
    -   **SonarQube:** [http://localhost:9000](http://localhost:9000) (Default login: `admin` / `admin`)

## Local Development Setup

If you prefer to run the Python code locally while keeping services in Docker:

1.  **Start Infrastructure (Mongo & SonarQube):**
    ```bash
    docker-compose up -d mongo sonarqube
    ```

2.  **Install Dependencies (using uv):**
    ```bash
    uv sync
    ```
    Or using pip:
    ```bash
    pip install -r requirements.txt
    ```

3.  **Configure Environment:**
    Set the following environment variables (or modify `config.py`):
    -   `MONGO_URI`: `mongodb://localhost:27017`
    -   `SONAR_HOST_URL`: `http://localhost:9000`
    -   `SONAR_TOKEN`: Your SonarQube token (generate one in SonarQube UI).
    -   `GITHUB_TOKENS`: Comma-separated list of GitHub tokens.
    -   `SONAR_SCANNER_BIN`: Path to your local `sonar-scanner` executable.

4.  **Run the API Server:**
    ```bash
    uv run uvicorn main:app --reload
    ```
    Or with python:
    ```bash
    python -m uvicorn main:app --reload
    ```

## Usage

### Via Web API
Go to [http://localhost:8000/docs](http://localhost:8000/docs) and use the `/scan` endpoint to trigger a scan.

### Via CLI (Legacy)
You can still run the scanner directly on a CSV file:

```bash
uv run scan_manager.py path/to/your/data.csv
```

The CSV should have:
-   `repo_url`: Full git URL.
-   `commit_sha`: Commit hash to scan.
-   `project_key` (Optional).
