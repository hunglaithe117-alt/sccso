import os

class Config:
    SONAR_HOST_URL = os.getenv("SONAR_HOST_URL", "http://localhost:9000")
    SONAR_TOKEN = os.getenv("SONAR_TOKEN", "admin") # Default or change as needed
    WORK_DIR = os.getenv("WORK_DIR", "/Users/hunglai/hust/20251/thesis/scan/work_dir")
    SONAR_SCANNER_BIN = os.getenv("SONAR_SCANNER_BIN", "sonar-scanner")
    GITHUB_TOKENS = os.getenv("GITHUB_TOKENS", "").split(",")
    
    # Batch scan configuration
    CONCURRENT_SCANS = int(os.getenv("CONCURRENT_SCANS", "4"))
    BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50"))
    CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", "scan_checkpoint.json")
    INPUT_CSV = os.getenv("INPUT_CSV", "commits_to_scan.csv")

