import logging
from collections import deque
from datetime import datetime
from pathlib import Path
from threading import Condition, Lock, Thread
from typing import Dict, List, Tuple
from uuid import uuid4

import pandas as pd
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse

from config import Config
from scan_manager import MiniScanner

logger = logging.getLogger(__name__)

# Initialize scanner and storage
scanner = MiniScanner()
scanner.checkpoint.reset_upload_states()
UPLOAD_DIR = Path(Config.WORK_DIR) / "uploads"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="Mini Scanner UI", version="1.0.0")

# Simple in-memory job store
jobs: Dict[str, Dict] = {}
uploads: Dict[str, Dict] = {}
scan_lock = Lock()
job_queue: deque[Tuple[str, Path, str]] = deque()
queue_cv = Condition()


def _load_uploads_from_db():
    uploads.clear()
    for row in scanner.checkpoint.get_uploads():
        uploads[row["id"]] = row


_load_uploads_from_db()


def _job_worker():
    while True:
        with queue_cv:
            while not job_queue:
                queue_cv.wait()
            job_id, csv_path, upload_id = job_queue.popleft()

        jobs[job_id]["status"] = "running"
        jobs[job_id]["started_at"] = datetime.utcnow().isoformat() + "Z"
        if upload_id and upload_id in uploads:
            uploads[upload_id]["status"] = "running"
            uploads[upload_id]["job_id"] = job_id
            scanner.checkpoint.update_upload_status(upload_id, status="running", job_id=job_id)

        try:
            scanner.check_dependencies()
            with scan_lock:
                scanner.process_csv(csv_path)
            jobs[job_id]["status"] = "completed"
            jobs[job_id]["completed_at"] = datetime.utcnow().isoformat() + "Z"
            if upload_id and upload_id in uploads:
                uploads[upload_id]["status"] = "completed"
                uploads[upload_id]["job_id"] = job_id
                scanner.checkpoint.update_upload_status(upload_id, status="completed", job_id=job_id, error=None)
        except Exception as exc:
            logger.exception("Scan failed", exc_info=exc)
            jobs[job_id]["status"] = "error"
            jobs[job_id]["error"] = str(exc)
            jobs[job_id]["completed_at"] = datetime.utcnow().isoformat() + "Z"
            if upload_id and upload_id in uploads:
                uploads[upload_id]["status"] = "error"
                uploads[upload_id]["error"] = str(exc)
                uploads[upload_id]["job_id"] = job_id
                scanner.checkpoint.update_upload_status(upload_id, status="error", job_id=job_id, error=str(exc))


Thread(target=_job_worker, daemon=True).start()


def _sanitize_filename(filename: str) -> str:
    return Path(filename).name or "upload.csv"


def start_scan(csv_path: Path, upload_id: str = None) -> str:
    job_id = str(uuid4())
    jobs[job_id] = {
        "status": "queued",
        "csv_path": str(csv_path),
        "created_at": datetime.utcnow().isoformat() + "Z",
        "upload_id": upload_id,
    }
    with queue_cv:
        job_queue.append((job_id, csv_path, upload_id))
        queue_cv.notify()
    return job_id


def summarize_csv(csv_path: Path) -> Dict:
    """
    Return per-repo commit counts for a CSV.
    """
    summary = {}
    total = 0
    try:
        for chunk in pd.read_csv(csv_path, chunksize=Config.BATCH_SIZE):
            for row in chunk.to_dict("records"):
                repo_url = row.get("repo_url")
                gh_project_name = row.get("gh_project_name")
                if gh_project_name and not repo_url:
                    repo_url = f"https://github.com/{gh_project_name}.git"
                if not repo_url:
                    continue

                slug = repo_url.split("github.com/")[-1].replace(".git", "") if "github.com" in repo_url else None
                owner = None
                repo_name = repo_url.split("/")[-1].replace(".git", "")
                if slug and "/" in slug:
                    owner, repo_name = slug.split("/", 1)
                key = f"{owner}_{repo_name}" if owner else repo_name
                summary[key] = summary.get(key, 0) + 1
                total += 1
    except Exception as exc:
        logger.warning(f"Failed to summarize CSV {csv_path}: {exc}")

    rows = [
        {"repo": repo, "commits": commits}
        for repo, commits in sorted(summary.items(), key=lambda kv: kv[0])
    ]
    return {"total_commits": total, "repos": rows}


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
    .row { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
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
    table {
      width: 100%;
      border-collapse: collapse;
      margin-top: 8px;
    }
    th, td {
      padding: 10px 12px;
      border-bottom: 1px solid rgba(255,255,255,0.08);
      text-align: left;
    }
    th { color: var(--muted); font-size: 14px; }
    tr:last-child td { border-bottom: none; }
    .pill {
      display: inline-block;
      padding: 4px 8px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 700;
      color: #04101f;
    }
    .pill.success { background: #34d399; }
    .pill.fail { background: #f87171; }
    .pill.pending { background: #fbbf24; }
    .pill.total { background: #93c5fd; }
    .muted { color: var(--muted); font-size: 14px; }
    a { color: var(--accent); }
    .flex-between { display:flex; justify-content:space-between; align-items:center; gap:10px; flex-wrap:wrap; }
    .refresh { font-size: 13px; cursor: pointer; color: var(--accent); }
  </style>
</head>
<body>
  <div class="shell">
    <h1>Mini Scanner</h1>
    <p>Tải CSV, chọn scan và xem trạng thái ngay trong trình duyệt.</p>

    <div class="block">
      <label for="csv-file">CSV files</label>
      <input id="csv-file" type="file" accept=".csv" multiple />
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

    <div class="block">
      <div class="flex-between">
        <div>
          <label>Uploads</label>
          <p class="muted" style="margin:4px 0 0;">Danh sách repo/commit trong các CSV đã tải. Nhấn Thu thập để lấy danh sách, Scan để chạy từng file, hoặc Scan tất cả (chạy tuần tự).</p>
        </div>
        <div class="row" style="gap:8px;">
          <button id="scan-all-btn">Scan tất cả chưa chạy</button>
          <span class="refresh" id="refresh-uploads">↻ Thu thập</span>
        </div>
      </div>
      <div style="overflow-x:auto;">
        <table id="uploads-table">
          <thead>
            <tr>
              <th>Upload</th>
              <th>Repos</th>
              <th>Tổng commits</th>
              <th>Trạng thái</th>
              <th>Hành động</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </div>

    <div class="block">
      <div class="flex-between">
        <div>
          <label>Trạng thái từng repo</label>
          <p class="muted" style="margin:4px 0 0;">Hiển thị số commit theo repo (done / failed / pending). Nhấn làm mới để cập nhật.</p>
        </div>
        <span class="refresh" id="refresh-btn">↻ Làm mới</span>
      </div>
      <div id="repo-summary" class="status" style="min-height: 32px;">Đang tải...</div>
      <div style="overflow-x:auto;">
        <table id="repo-table">
          <thead>
            <tr>
              <th>Repo</th>
              <th>Tổng</th>
              <th>Thành công</th>
              <th>Thất bại</th>
              <th>Đang chạy</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </div>

    <p class="muted">Work dir: {work_dir}</p>
  </div>

  <script>
    const uploadBtn = document.getElementById('upload-btn');
    const uploadResult = document.getElementById('upload-result');
    const jobStatus = document.getElementById('job-status');
    const jobIdInput = document.getElementById('job-id');
    const repoSummaryEl = document.getElementById('repo-summary');
    const repoTableBody = document.querySelector('#repo-table tbody');
    const refreshBtn = document.getElementById('refresh-btn');
    const uploadsTableBody = document.querySelector('#uploads-table tbody');
    const scanAllBtn = document.getElementById('scan-all-btn');
    const refreshUploadsBtn = document.getElementById('refresh-uploads');
    let activeUploadJobs = [];

    function showStatus(el, msg, error=false) {
      el.textContent = msg;
      el.style.borderColor = error ? 'rgba(255,99,99,0.6)' : 'rgba(255,255,255,0.08)';
    }

    uploadBtn.addEventListener('click', async () => {
      const fileInput = document.getElementById('csv-file');
      if (!fileInput.files.length) {
        showStatus(uploadResult, 'Please choose at least one CSV file.', true);
        return;
      }

      const form = new FormData();
      for (const file of fileInput.files) {
        form.append('files', file);
      }

      showStatus(uploadResult, 'Uploading...');
      try {
        const res = await fetch('/api/upload', { method: 'POST', body: form });
        if (!res.ok) {
          const errText = await res.text();
          showStatus(uploadResult, `Upload failed: ${errText}`, true);
          return;
        }
        const data = await res.json();
        if (!data.results || !data.results.length) {
          showStatus(uploadResult, 'No files processed.', true);
          return;
        }
        const lines = [];
        data.results.forEach(item => {
          lines.push(`Saved ${item.filename} -> ${item.saved_as}`);
        });
        showStatus(uploadResult, lines.join('\\n'));
        loadUploads();
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

    async function pollJobs(jobIds, auto=true) {
      if (!jobIds.length) return;
      try {
        const statuses = await Promise.all(jobIds.map(async (id) => {
          const res = await fetch(`/api/jobs/${id}`);
          if (!res.ok) return { id, error: await res.text() };
          return { id, ...(await res.json()) };
        }));
        const lines = statuses.map(s => {
          if (s.error) return `Job ${s.id}: ${s.error}`;
          return `Job ${s.id}: ${s.status}${s.error ? ' - ' + s.error : ''}`;
        });
        showStatus(jobStatus, lines.join('\\n'));
        const hasActive = statuses.some(s => s.status === 'queued' || s.status === 'running');
        if (auto && hasActive) {
          setTimeout(() => pollJobs(jobIds, true), 2000);
        }
      } catch (err) {
        showStatus(jobStatus, `Error: ${err}`, true);
      }
    }

    async function loadUploads() {
      try {
        const res = await fetch('/api/uploads');
        if (!res.ok) {
          uploadsTableBody.innerHTML = '<tr><td colspan="5" style="color:#f87171;">Lỗi tải danh sách uploads</td></tr>';
          return;
        }
        const data = await res.json();
        uploadsTableBody.innerHTML = '';
        if (!data.length) {
          uploadsTableBody.innerHTML = '<tr><td colspan="5" style="text-align:center;color:var(--muted);">Chưa có upload nào</td></tr>';
          return;
        }
        data.forEach(u => {
          const reposText = (u.repos || []).map(r => `${r.repo} (${r.commits})`).join(', ');
          const tr = document.createElement('tr');
          const disabled = u.status === 'queued' || u.status === 'running';
          tr.innerHTML = `
            <td>${u.filename}</td>
            <td>${reposText || '(unknown)'}</td>
            <td>${u.total_commits || 0}</td>
            <td>${u.status || 'uploaded'}</td>
            <td>
              <button data-upload="${u.id}" ${disabled ? 'disabled' : ''}>Scan</button>
            </td>
          `;
          tr.querySelector('button').addEventListener('click', () => triggerScan(u.id));
          uploadsTableBody.appendChild(tr);
        });
      } catch (err) {
        uploadsTableBody.innerHTML = `<tr><td colspan="5" style="color:#f87171;">${err}</td></tr>`;
      }
    }

    async function triggerScan(uploadId) {
      showStatus(jobStatus, 'Starting scan...');
      try {
        const res = await fetch(`/api/uploads/${uploadId}/scan`, { method: 'POST' });
        if (!res.ok) {
          const txt = await res.text();
          showStatus(jobStatus, `Không thể scan: ${txt}`, true);
          return;
        }
        const data = await res.json();
        showStatus(jobStatus, `Job ${data.job_id} queued`);
        loadUploads();
        pollJob(data.job_id, true);
      } catch (err) {
        showStatus(jobStatus, `Error: ${err}`, true);
      }
    }

    async function triggerScanAll() {
      showStatus(jobStatus, 'Starting all pending scans...');
      try {
        const res = await fetch('/api/uploads/scan_all_pending', { method: 'POST' });
        if (!res.ok) {
          const txt = await res.text();
          showStatus(jobStatus, `Không thể scan tất cả: ${txt}`, true);
          return;
        }
        const data = await res.json();
        if (!data.job_ids || !data.job_ids.length) {
          showStatus(jobStatus, 'Không có upload pending để scan.');
          return;
        }
        showStatus(jobStatus, `Queued jobs: ${data.job_ids.join(', ')}`);
        loadUploads();
        pollJobs(data.job_ids, true);
      } catch (err) {
        showStatus(jobStatus, `Error: ${err}`, true);
      }
    }

    scanAllBtn.addEventListener('click', triggerScanAll);
    refreshUploadsBtn.addEventListener('click', loadUploads);
    loadUploads();

    async function loadRepoSummary() {
      repoSummaryEl.textContent = 'Đang tải...';
      try {
        const res = await fetch('/api/repos');
        if (!res.ok) {
          const txt = await res.text();
          repoSummaryEl.textContent = `Lỗi: ${txt}`;
          return;
        }
        const data = await res.json();
        const totals = data.reduce((acc, row) => {
          acc.total += row.total;
          acc.processed += row.processed;
          acc.failed += row.failed;
          acc.pending += row.pending;
          return acc;
        }, { total:0, processed:0, failed:0, pending:0 });
        repoSummaryEl.textContent = `Tổng: ${totals.total} | Thành công: ${totals.processed} | Thất bại: ${totals.failed} | Đang chạy: ${totals.pending}`;

        repoTableBody.innerHTML = '';
        if (!data.length) {
          repoTableBody.innerHTML = '<tr><td colspan="5" style="text-align:center;color:var(--muted);">Chưa có dữ liệu</td></tr>';
          return;
        }

        data.forEach(row => {
          const tr = document.createElement('tr');
          tr.innerHTML = `
            <td>${row.repo_name || '(unknown)'}</td>
            <td><span class="pill total">${row.total}</span></td>
            <td><span class="pill success">${row.processed}</span></td>
            <td><span class="pill fail">${row.failed}</span></td>
            <td><span class="pill pending">${row.pending}</span></td>
          `;
          repoTableBody.appendChild(tr);
        });
      } catch (err) {
        repoSummaryEl.textContent = `Error: ${err}`;
      }
    }

    refreshBtn.addEventListener('click', loadRepoSummary);
    loadRepoSummary();
  </script>
</body>
</html>
        """.replace("{work_dir}", str(Config.WORK_DIR))
    )


@app.post("/api/upload")
async def upload_csv(
    files: List[UploadFile] = File(...),
):
    if not files:
        raise HTTPException(status_code=400, detail="Please upload at least one CSV file.")

    results = []
    timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    for idx, file in enumerate(files, start=1):
        filename = _sanitize_filename(file.filename)
        if not filename.lower().endswith(".csv"):
            raise HTTPException(status_code=400, detail=f"File {filename} is not CSV.")

        destination = UPLOAD_DIR / f"{timestamp}-{idx}-{filename}"
        with destination.open("wb") as buffer:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                buffer.write(chunk)

        summary = summarize_csv(destination)
        upload_id = str(uuid4())
        uploads[upload_id] = {
            "id": upload_id,
            "filename": filename,
            "saved_as": str(destination),
            "status": "uploaded",
            "repos": summary.get("repos", []),
            "total_commits": summary.get("total_commits", 0),
            "uploaded_at": datetime.utcnow().isoformat() + "Z",
            "job_id": None,
            "error": None,
        }
        scanner.checkpoint.upsert_upload(uploads[upload_id])

        entry = {
            "filename": filename,
            "saved_as": str(destination),
            "scan_started": False,
            "upload_id": upload_id,
            "repos": summary.get("repos", []),
            "total_commits": summary.get("total_commits", 0),
        }

        results.append(entry)

    return JSONResponse({"results": results})


@app.get("/api/jobs")
async def list_jobs():
    return JSONResponse(jobs)


@app.get("/api/jobs/{job_id}")
async def get_job(job_id: str):
    job = jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return JSONResponse(job)


@app.get("/api/repos")
async def repo_summary():
    """
    Return aggregate commit counts per repo using the checkpoint DB.
    """
    return JSONResponse(scanner.checkpoint.get_repo_summary())


@app.get("/api/uploads")
async def list_uploads():
    _load_uploads_from_db()
    data = sorted(
        uploads.values(),
        key=lambda u: u.get("uploaded_at", ""),
        reverse=True,
    )
    return JSONResponse(data)


def _can_start_upload(upload: Dict) -> bool:
    return upload.get("status") not in {"queued", "running", "completed"}


@app.post("/api/uploads/{upload_id}/scan")
async def scan_upload(upload_id: str):
    _load_uploads_from_db()
    upload = uploads.get(upload_id)
    if not upload:
        raise HTTPException(status_code=404, detail="Upload not found")
    if not _can_start_upload(upload):
        return JSONResponse({"message": "Upload already queued/running/completed", "job_id": upload.get("job_id")})

    job_id = start_scan(Path(upload["saved_as"]), upload_id=upload_id)
    upload["status"] = "queued"
    upload["job_id"] = job_id
    scanner.checkpoint.update_upload_status(upload_id, status="queued", job_id=job_id)
    return JSONResponse({"job_id": job_id})


@app.post("/api/uploads/scan_all_pending")
async def scan_all_pending():
    _load_uploads_from_db()
    job_ids = []
    for upload in uploads.values():
        if _can_start_upload(upload):
            job_id = start_scan(Path(upload["saved_as"]), upload_id=upload["id"])
            upload["status"] = "queued"
            upload["job_id"] = job_id
            scanner.checkpoint.update_upload_status(upload["id"], status="queued", job_id=job_id)
            job_ids.append(job_id)
    return JSONResponse({"job_ids": job_ids})
