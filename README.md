# Mini SonarQube Scanner (Batch)

This is a tool to perform batch scanning of git repositories at specific commits using SonarQube. It supports concurrent scanning and checkpointing to resume interrupted jobs.

## Prerequisites

-   **Docker** and **Docker Compose** (Recommended)
-   **Python 3.11+** (If running locally)

## Quick Start (Docker)

1.  **Configuration:**
    Copy `.env.example` to `.env` and update the values.
    ```bash
    cp .env.example .env
    ```
    *   `GITHUB_TOKENS`: Required if you need to scan commits that might only exist in forks (the tool attempts to replay them).
    *   `SONAR_TOKEN`: You can leave as `admin` initially, but should update after setting up SonarQube if you change the password.

2.  **Start Services:**
    This starts SonarQube and a Scanner container.
    ```bash
    docker-compose up -d --build
    ```
    *   Access SonarQube at [http://localhost:9000](http://localhost:9000) (Default: `admin` / `admin`).
    *   **Note:** Wait for SonarQube to fully start before running scans.

3.  **Use the web UI (no CLI needed):**
    Open [http://localhost:8000](http://localhost:8000) and drop a CSV. The UI will upload it into `work_dir/uploads/` inside the container and can start a scan immediately. You can paste the returned Job ID to poll status.
    
    **API endpoints (served by the scanner container):**
    *   `POST /api/upload` (form-data: `file`, `scan_now=true|false`) → saves CSV to `/app/work_dir/uploads/...` and optionally starts a scan, returns `job_id`.
    *   `GET /api/jobs/{job_id}` → current status for that job.
    *   `GET /api/jobs` → list all tracked jobs (in-memory).

4.  **(Optional) CLI flow:**
    Place your CSV file (e.g., `commits.csv`) in the `work_dir/` folder at the repo root. This folder is mounted into the scanner container at `/app/work_dir`.
    *   Verify the mount from inside the container (only lists files, does not run scans):
        ```bash
        docker-compose exec scanner python3 scan_manager.py --list-workdir
        ```
    *   Run the scanner manually:
        ```bash
        docker-compose exec scanner python3 scan_manager.py work_dir/commits.csv
        ```
        Or, if `INPUT_CSV` is set in `.env` (pointing to a file in `work_dir/`):
        ```bash
        docker-compose exec scanner python3 scan_manager.py
        ```

## Local Development Setup

If you want to run the Python script locally (outside Docker) but keep SonarQube in Docker:

1.  **Start SonarQube:**
    ```bash
    docker-compose up -d sonarqube
    ```

2.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
    *   You also need `sonar-scanner` installed and in your PATH.

3.  **Run Scanner:**
    Set environment variables (or rely on defaults/config.py) and run:
    ```bash
    export SONAR_HOST_URL="http://localhost:9000"
    export SONAR_TOKEN="admin"
    export CONCURRENT_SCANS=4
    
    python3 scan_manager.py path/to/your/commits.csv
    ```

## Configuration

You can configure the scanner via environment variables or `.env` file (for Docker):

| Variable | Description | Default |
| :--- | :--- | :--- |
| `SONAR_HOST_URL` | URL of the SonarQube server | `http://localhost:9000` |
| `SONAR_TOKEN` | Authentication token for SonarQube | `admin` |
| `SONAR_EXCLUSIONS` | Comma-separated glob patterns to skip (build/output dirs) | `.git/**,**/node_modules/**,**/build/**,**/dist/**,**/target/**,**/.gradle/**,**/.idea/**` |
| `GITHUB_TOKENS` | Comma-separated GitHub tokens for API access | (Empty) |
| `CONCURRENT_SCANS` | Number of parallel scans to run | `4` |
| `BATCH_SIZE` | Number of rows to read/process at a time | `50` |
| `CHECKPOINT_FILE` | File to store progress (relative paths resolve inside `WORK_DIR`) | `work_dir/scan_checkpoint.json` |
| `WORK_DIR` | Location for temporary repos, checkpoints, and CSV input (mounted for Docker) | `<repo>/work_dir` (overridden to `/app/work_dir` in docker-compose) |
| `INPUT_CSV` | Default CSV file name/path to use when no CLI arg is provided | `commits_to_scan.csv` |

## Checkpoints

The scanner automatically creates a `scan_checkpoint.json` file. 
-   **Resuming:** If you stop the script and run it again with the same CSV, it will skip commits that are already marked as processed in the checkpoint file.
-   **Retrying:** To retry failed commits, you can manually edit the checkpoint file or simply re-run; currently, failed items are recorded but not automatically skipped on next run unless you implement specific logic, or they are just re-attempted if not in "processed" list. (Note: The current implementation marks them as failed in the checkpoint but doesn't add them to 'processed', so they *will* be retried on the next run).
