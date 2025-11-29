import logging
from datetime import datetime
from pathlib import Path
from threading import Lock, Thread
from typing import Dict
from uuid import uuid4

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse

from config import Config
from scan_manager import MiniScanner

logger = logging.getLogger(__name__)

# Initialize scanner and storage
scanner = MiniScanner()
UPLOAD_DIR = Path(Config.WORK_DIR) / "uploads"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="Mini Scanner UI", version="1.0.0")

# Simple in-memory job store
jobs: Dict[str, Dict] = {}
scan_lock = Lock()


def _sanitize_filename(filename: str) -> str:
    return Path(filename).name or "upload.csv"


def _run_scan(job_id: str, csv_path: Path):
    """
    Run the scan in a background thread to keep the API responsive.
    """
    jobs[job_id]["status"] = "running"
    jobs[job_id]["started_at"] = datetime.utcnow().isoformat() + "Z"
    try:
        scanner.check_dependencies()
        with scan_lock:
            scanner.process_csv(csv_path)
        jobs[job_id]["status"] = "completed"
        jobs[job_id]["completed_at"] = datetime.utcnow().isoformat() + "Z"
    except Exception as exc:
        logger.exception("Scan failed", exc_info=exc)
        jobs[job_id]["status"] = "error"
        jobs[job_id]["error"] = str(exc)
        jobs[job_id]["completed_at"] = datetime.utcnow().isoformat() + "Z"


def start_scan(csv_path: Path) -> str:
    job_id = str(uuid4())
    jobs[job_id] = {
        "status": "queued",
        "csv_path": str(csv_path),
        "created_at": datetime.utcnow().isoformat() + "Z",
    }
    Thread(target=_run_scan, args=(job_id, csv_path), daemon=True).start()
    return job_id


@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse(
        """
<!doctype html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Mini Scanner</title>
  <style>
    :root {
      --bg: #0f172a;
      --card: #111827;
      --accent: #22d3ee;
      --muted: #94a3b8;
      --text: #e2e8f0;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      min-height: 100vh;
      background: radial-gradient(circle at 20% 20%, #1d2a4a 0, #0f172a 35%, #0b1221 100%);
      color: var(--text);
      font-family: "Segoe UI", "Helvetica Neue", Arial, sans-serif;
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 24px;
    }
    .shell {
      width: 100%;
      max-width: 720px;
      background: var(--card);
      border: 1px solid rgba(255,255,255,0.08);
      border-radius: 16px;
      padding: 24px;
      box-shadow: 0 20px 40px rgba(0,0,0,0.35);
    }
    h1 { margin: 0 0 12px; letter-spacing: 0.4px; }
    p { margin: 0 0 16px; color: var(--muted); }
    .block {
      border: 1px dashed rgba(255,255,255,0.18);
      border-radius: 12px;
      padding: 16px;
      margin-bottom: 16px;
      background: rgba(255,255,255,0.02);
    }
    label { display: block; margin-bottom: 8px; font-weight: 600; }
    input[type="file"] { color: var(--text); }
    button {
      margin-top: 10px;
      padding: 10px 16px;
      border: none;
      border-radius: 10px;
      background: linear-gradient(135deg, #22d3ee, #0ea5e9);
      color: #04101f;
      font-weight: 700;
      cursor: pointer;
      transition: transform 0.08s ease, box-shadow 0.1s ease;
    }
    button:active { transform: translateY(1px); box-shadow: 0 4px 12px rgba(0,0,0,0.35); }
    .row { display: flex; align-items: center; gap: 10px; }
    input[type="checkbox"] { width: 18px; height: 18px; }
    .status {
      background: #0b1221;
      border-radius: 10px;
      padding: 10px 12px;
      margin-top: 8px;
      font-family: "SFMono-Regular", Consolas, "Liberation Mono", monospace;
      color: #e2e8f0;
      border: 1px solid rgba(255,255,255,0.05);
      white-space: pre-wrap;
      min-height: 48px;
    }
    .muted { color: var(--muted); font-size: 14px; }
    a { color: var(--accent); }
  </style>
</head>
<body>
  <div class="shell">
    <h1>Mini Scanner</h1>
    <p>Tải CSV, chọn scan và xem trạng thái ngay trong trình duyệt.</p>

    <div class="block">
      <label for="csv-file">CSV file</label>
      <input id="csv-file" type="file" accept=".csv" />
      <div class="row">
        <input id="scan-now" type="checkbox" checked />
        <span class="muted">Start scan after upload</span>
      </div>
      <button id="upload-btn">Upload</button>
      <div id="upload-result" class="status"></div>
    </div>

    <div class="block">
      <label for="job-id">Job status</label>
      <div class="row">
        <input id="job-id" type="text" placeholder="Job ID..." style="flex:1;padding:8px;border-radius:8px;border:1px solid rgba(255,255,255,0.08);background:#0b1221;color:#e2e8f0;">
        <button id="check-btn">Check</button>
      </div>
      <div id="job-status" class="status"></div>
    </div>

    <p class="muted">Work dir: {work_dir}</p>
  </div>

  <script>
    const uploadBtn = document.getElementById('upload-btn');
    const uploadResult = document.getElementById('upload-result');
    const jobStatus = document.getElementById('job-status');
    const jobIdInput = document.getElementById('job-id');

    function showStatus(el, msg, error=false) {
      el.textContent = msg;
      el.style.borderColor = error ? 'rgba(255,99,99,0.6)' : 'rgba(255,255,255,0.08)';
    }

    uploadBtn.addEventListener('click', async () => {
      const fileInput = document.getElementById('csv-file');
      const scanNow = document.getElementById('scan-now').checked;
      if (!fileInput.files.length) {
        showStatus(uploadResult, 'Please choose a CSV file.', true);
        return;
      }

      const form = new FormData();
      form.append('file', fileInput.files[0]);
      form.append('scan_now', scanNow ? 'true' : 'false');

      showStatus(uploadResult, 'Uploading...');
      try {
        const res = await fetch('/api/upload', { method: 'POST', body: form });
        if (!res.ok) {
          const errText = await res.text();
          showStatus(uploadResult, `Upload failed: ${errText}`, true);
          return;
        }
        const data = await res.json();
        let msg = `Saved to: ${data.saved_as}`;
        if (data.scan_started) {
          msg += `\\nJob ID: ${data.job_id}\\nStatus: queued`;
          jobIdInput.value = data.job_id;
          pollJob(data.job_id);
        }
        showStatus(uploadResult, msg);
      } catch (err) {
        showStatus(uploadResult, `Error: ${err}`, true);
      }
    });

    document.getElementById('check-btn').addEventListener('click', () => {
      const jobId = jobIdInput.value.trim();
      if (!jobId) {
        showStatus(jobStatus, 'Enter a job ID to check.', true);
        return;
      }
      pollJob(jobId, false);
    });

    async function pollJob(jobId, auto=false) {
      showStatus(jobStatus, 'Loading...');
      try {
        const res = await fetch(`/api/jobs/${jobId}`);
        if (!res.ok) {
          const txt = await res.text();
          showStatus(jobStatus, `Not found or error: ${txt}`, true);
          return;
        }
        const data = await res.json();
        showStatus(jobStatus, JSON.stringify(data, null, 2));
        if (auto && (data.status === 'queued' || data.status === 'running')) {
          setTimeout(() => pollJob(jobId, true), 2000);
        }
      } catch (err) {
        showStatus(jobStatus, `Error: ${err}`, true);
      }
    }
  </script>
</body>
</html>
        """.replace("{work_dir}", str(Config.WORK_DIR))
    )


@app.post("/api/upload")
async def upload_csv(
    file: UploadFile = File(...),
    scan_now: bool = Form(True),
):
    filename = _sanitize_filename(file.filename)
    if not filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Please upload a CSV file.")

    destination = UPLOAD_DIR / f"{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}-{filename}"
    with destination.open("wb") as buffer:
        while True:
            chunk = await file.read(1024 * 1024)
            if not chunk:
                break
            buffer.write(chunk)

    response = {
        "saved_as": str(destination),
        "scan_started": False,
    }

    if scan_now:
        job_id = start_scan(destination)
        response.update({"scan_started": True, "job_id": job_id})

    return JSONResponse(response)


@app.get("/api/jobs")
async def list_jobs():
    return JSONResponse(jobs)


@app.get("/api/jobs/{job_id}")
async def get_job(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return JSONResponse(job)
