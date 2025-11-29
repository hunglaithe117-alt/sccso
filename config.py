import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent


class Config:
    SONAR_HOST_URL = os.getenv("SONAR_HOST_URL", "http://localhost:9000")
    SONAR_TOKEN = os.getenv("SONAR_TOKEN", "admin")  # Default or change as needed
    WORK_DIR = os.getenv("WORK_DIR", str(BASE_DIR / "work_dir"))
    SONAR_SCANNER_BIN = os.getenv("SONAR_SCANNER_BIN", "sonar-scanner")
    SONAR_EXCLUSIONS = os.getenv(
        "SONAR_EXCLUSIONS",
        ".git/**,**/node_modules/**,**/build/**,**/dist/**,**/target/**,**/.gradle/**,**/.idea/**",
    )
    GITHUB_TOKENS = os.getenv("GITHUB_TOKENS", "").split(",")

    # Batch scan configuration
    CONCURRENT_SCANS = int(os.getenv("CONCURRENT_SCANS", "4"))
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
    _checkpoint_env = os.getenv("CHECKPOINT_FILE")
    if _checkpoint_env:
        _checkpoint_path = Path(_checkpoint_env)
        CHECKPOINT_FILE = (
            str(_checkpoint_path)
            if _checkpoint_path.is_absolute()
            else str(Path(WORK_DIR) / _checkpoint_path)
        )
    else:
        CHECKPOINT_FILE = str(Path(WORK_DIR) / "scan_checkpoint.db")
    INPUT_CSV = os.getenv("INPUT_CSV", "commits_to_scan.csv")
