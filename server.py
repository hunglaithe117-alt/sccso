import logging
import shutil
import tempfile
from pathlib import Path
from fastapi import FastAPI, Request, UploadFile, File, BackgroundTasks, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from typing import List

from db import Repository
from models import ProjectStatus, ScanJobStatus
from scan_manager import MiniScanner

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(title="Mini BuildGuard")
templates = Jinja2Templates(directory="templates")

# Initialize Repository and Scanner
repo = Repository()
scanner = MiniScanner()

# --- Background Tasks ---

def process_csv_task(file_path: str):
    """Background task to process the uploaded CSV."""
    try:
        scanner.process_csv(file_path)
    except Exception as e:
        logger.error(f"Error processing CSV {file_path}: {e}")
    finally:
        # Cleanup temp file
        Path(file_path).unlink(missing_ok=True)

def run_scan_job_task(job_id: str):
    """Background task to run a single scan job."""
    job = repo.scan_jobs_col.find_one({"_id": repo._object_id(job_id)})
    if not job:
        return

    repo.update_scan_job(job_id, ScanJobStatus.running)
    
    try:
        repo_url = job["repository_url"]
        commit_sha = job["commit_sha"]
        project_key = job["project_key"]
        repo_name = repo_url.split('/')[-1].replace('.git', '')

        repo_path = scanner.ensure_repo(repo_url, repo_name)
        scanner.checkout_commit(repo_path, commit_sha)
        success = scanner.run_sonar_scan(repo_path, project_key, commit_sha)
        
        if success:
            repo.update_scan_job(job_id, ScanJobStatus.success)
        else:
            repo.update_scan_job(job_id, ScanJobStatus.failed_temp, error="Scanner command failed")
            
    except Exception as e:
        logger.error(f"Scan job {job_id} failed: {e}")
        repo.update_scan_job(job_id, ScanJobStatus.failed_permanent, error=str(e))

# --- API Routes ---

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("projects.html", {"request": request})

@app.get("/jobs", response_class=HTMLResponse)
async def jobs_page(request: Request):
    return templates.TemplateResponse("jobs.html", {"request": request})

@app.get("/api/projects")
async def list_projects():
    projects = list(repo.projects_col.find().sort("created_at", -1).limit(50))
    return [repo._serialize(p) for p in projects]

@app.post("/api/upload")
async def upload_csv(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed")
    
    # Save to temp file
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
        shutil.copyfileobj(file.file, tmp)
        tmp_path = tmp.name
    
    # Trigger background processing
    background_tasks.add_task(process_csv_task, tmp_path)
    
    return {"message": "File uploaded and processing started", "filename": file.filename}

@app.get("/api/jobs")
async def list_jobs():
    jobs = list(repo.scan_jobs_col.find().sort("created_at", -1).limit(100))
    return [repo._serialize(j) for j in jobs]

@app.post("/api/jobs/{job_id}/retry")
async def retry_job(job_id: str, background_tasks: BackgroundTasks):
    job = repo.scan_jobs_col.find_one({"_id": repo._object_id(job_id)})
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    
    # Reset status
    repo.update_scan_job(job_id, ScanJobStatus.pending, error=None)
    
    # Trigger background scan
    background_tasks.add_task(run_scan_job_task, job_id)
    
    return {"message": "Job queued for retry"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
