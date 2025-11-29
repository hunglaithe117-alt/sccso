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

3.  **Prepare Data:**
    Place your CSV file (e.g., `commits.csv`) in the `work_dir/` folder. This folder is mounted into the scanner container.
    
    **CSV Format:**
    ```csv
    repo_url,commit_sha,project_key
    https://github.com/user/repo.git,abcdef123456,my_project_key
    ```
    *   `project_key` is optional; if omitted, it defaults to `repo_name_commit_sha`.

4.  **Run Scan:**
    Execute the scanner inside the running container:
    ```bash
    docker-compose exec scanner python3 scan_manager.py work_dir/commits.csv
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
| `GITHUB_TOKENS` | Comma-separated GitHub tokens for API access | (Empty) |
| `CONCURRENT_SCANS` | Number of parallel scans to run | `4` |
| `BATCH_SIZE` | Number of rows to read/process at a time | `50` |
| `CHECKPOINT_FILE` | File to store progress | `scan_checkpoint.json` |

## Checkpoints

The scanner automatically creates a `scan_checkpoint.json` file. 
-   **Resuming:** If you stop the script and run it again with the same CSV, it will skip commits that are already marked as processed in the checkpoint file.
-   **Retrying:** To retry failed commits, you can manually edit the checkpoint file or simply re-run; currently, failed items are recorded but not automatically skipped on next run unless you implement specific logic, or they are just re-attempted if not in "processed" list. (Note: The current implementation marks them as failed in the checkpoint but doesn't add them to 'processed', so they *will* be retried on the next run).
