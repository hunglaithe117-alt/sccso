import json
import logging
import sqlite3
import time
from pathlib import Path
from typing import Dict, Optional, List

logger = logging.getLogger(__name__)


class CheckpointManager:
    def __init__(self, checkpoint_file: str):
        db_path = Path(checkpoint_file)
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db_path = str(db_path)
        self._init_db()

    def _get_conn(self):
        # timeout waits for locks briefly; WAL reduces contention for concurrent writers
        return sqlite3.connect(self.db_path, timeout=30)

    def _init_db(self):
        try:
            with self._get_conn() as conn:
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS scans (
                        commit_sha TEXT PRIMARY KEY,
                        status TEXT NOT NULL, -- 'PENDING', 'PROCESSED', 'FAILED'
                        error_msg TEXT,
                        repo_name TEXT,
                        project_key TEXT,
                        repo_url TEXT,
                        updated_at REAL
                    )
                    """
                )
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS uploads (
                        id TEXT PRIMARY KEY,
                        filename TEXT,
                        saved_as TEXT,
                        status TEXT,
                        total_commits INTEGER,
                        repos_json TEXT,
                        job_id TEXT,
                        error_msg TEXT,
                        uploaded_at TEXT
                    )
                    """
                )
                # Ensure columns AFTER both tables are created
                self._ensure_columns(conn)
                self._ensure_upload_columns(conn)
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_scans_repo ON scans(repo_name)"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_scans_status ON scans(status)"
                )
                conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_uploads_status ON uploads(status)"
                )
        except Exception as e:
            logger.error(f"Failed to init DB: {e}")

    def _ensure_columns(self, conn):
        """
        Add new columns if the DB was created before the schema was expanded.
        Only handles 'scans' table columns.
        """
        try:
            existing = {
                row[1] for row in conn.execute("PRAGMA table_info(scans)").fetchall()
            }
            extras = {
                "repo_name": "ALTER TABLE scans ADD COLUMN repo_name TEXT",
                "project_key": "ALTER TABLE scans ADD COLUMN project_key TEXT",
                "repo_url": "ALTER TABLE scans ADD COLUMN repo_url TEXT",
            }
            for col, ddl in extras.items():
                if col not in existing:
                    conn.execute(ddl)
        except Exception as e:
            logger.error(f"Failed to ensure scans schema: {e}")

    def _ensure_upload_columns(self, conn):
        """
        Add new columns to 'uploads' table if needed.
        """
        try:
            existing = {
                row[1] for row in conn.execute("PRAGMA table_info(uploads)").fetchall()
            }
            extras = {
                "total_commits": "ALTER TABLE uploads ADD COLUMN total_commits INTEGER",
                "repos_json": "ALTER TABLE uploads ADD COLUMN repos_json TEXT",
                "job_id": "ALTER TABLE uploads ADD COLUMN job_id TEXT",
                "uploaded_at": "ALTER TABLE uploads ADD COLUMN uploaded_at TEXT",
                "error_msg": "ALTER TABLE uploads ADD COLUMN error_msg TEXT",
            }
            for col, ddl in extras.items():
                if col not in existing:
                    conn.execute(ddl)
        except Exception as e:
            logger.error(f"Failed to ensure upload columns: {e}")

    def reset_pending_jobs(self):
        """Clear leftover pending jobs from previous runs so they can be claimed again."""
        try:
            with self._get_conn() as conn:
                conn.execute("DELETE FROM scans WHERE status = 'PENDING'")
            logger.info("Reset pending jobs from previous run.")
        except Exception as e:
            logger.error(f"Failed to reset pending jobs: {e}")

    def reset_upload_states(self):
        """
        Reset uploads stuck in queued/running back to 'uploaded' so they can be triggered again after restart.
        """
        try:
            with self._get_conn() as conn:
                conn.execute(
                    """
                    UPDATE uploads
                    SET status = 'uploaded', job_id = NULL, error_msg = NULL
                    WHERE status IN ('queued', 'running')
                    """
                )
        except Exception as e:
            logger.error(f"Failed to reset upload states: {e}")

    def try_claim_commit(
        self,
        commit_sha: str,
        repo_name: Optional[str] = None,
        project_key: Optional[str] = None,
        repo_url: Optional[str] = None,
    ) -> bool:
        """
        Attempt to claim a commit for processing.
        Returns True if claimed; False if already present (pending/processed/failed).
        """
        try:
            with self._get_conn() as conn:
                conn.execute(
                    """
                    INSERT INTO scans (commit_sha, status, repo_name, project_key, repo_url, updated_at)
                    VALUES (?, 'PENDING', ?, ?, ?, ?)
                    """,
                    (commit_sha, repo_name, project_key, repo_url, time.time()),
                )
            return True
        except sqlite3.IntegrityError:
            return False
        except Exception as e:
            logger.error(f"Error claiming commit {commit_sha}: {e}")
            return False

    def is_processed(self, commit_sha: str) -> bool:
        try:
            with self._get_conn() as conn:
                cursor = conn.execute(
                    "SELECT 1 FROM scans WHERE commit_sha = ? AND status = 'PROCESSED'",
                    (commit_sha,),
                )
                return cursor.fetchone() is not None
        except Exception:
            return False

    def mark_processed(
        self,
        commit_sha: str,
        repo_name: Optional[str] = None,
        project_key: Optional[str] = None,
        repo_url: Optional[str] = None,
    ):
        self._update_status(commit_sha, "PROCESSED", None, repo_name, project_key, repo_url)

    def mark_failed(
        self,
        commit_sha: str,
        error: str,
        repo_name: Optional[str] = None,
        project_key: Optional[str] = None,
        repo_url: Optional[str] = None,
    ):
        self._update_status(commit_sha, "FAILED", error, repo_name, project_key, repo_url)

    def _update_status(
        self,
        commit_sha: str,
        status: str,
        error: Optional[str] = None,
        repo_name: Optional[str] = None,
        project_key: Optional[str] = None,
        repo_url: Optional[str] = None,
    ):
        try:
            with self._get_conn() as conn:
                conn.execute(
                    """
                    UPDATE scans
                    SET status = ?, error_msg = ?, updated_at = ?,
                        repo_name = COALESCE(?, repo_name),
                        project_key = COALESCE(?, project_key),
                        repo_url = COALESCE(?, repo_url)
                    WHERE commit_sha = ?
                    """,
                    (
                        status,
                        error,
                        time.time(),
                        repo_name,
                        project_key,
                        repo_url,
                        commit_sha,
                    ),
                )
        except Exception as e:
            logger.error(f"Failed to update status for {commit_sha}: {e}")

    def get_stats(self) -> Dict[str, int]:
        try:
            with self._get_conn() as conn:
                rows = conn.execute(
                    "SELECT status, COUNT(*) FROM scans GROUP BY status"
                ).fetchall()
                return dict(rows)
        except Exception:
            return {}

    def get_repo_summary(self) -> List[Dict]:
        """
        Aggregate counts per repo for UI.
        """
        try:
            with self._get_conn() as conn:
                rows = conn.execute(
                    """
                    SELECT
                        COALESCE(repo_name, 'unknown') AS repo_name,
                        COUNT(*) AS total,
                        SUM(CASE WHEN status = 'PROCESSED' THEN 1 ELSE 0 END) AS processed,
                        SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed,
                        SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END) AS pending
                    FROM scans
                    GROUP BY COALESCE(repo_name, 'unknown')
                    ORDER BY repo_name
                    """
                ).fetchall()
                return [
                    {
                        "repo_name": row[0],
                        "total": row[1],
                        "processed": row[2],
                        "failed": row[3],
                        "pending": row[4],
                    }
                    for row in rows
                ]
        except Exception as e:
            logger.error(f"Failed to fetch repo summary: {e}")
            return []

    # Upload persistence helpers
    def upsert_upload(self, upload: Dict):
        try:
            with self._get_conn() as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO uploads
                    (id, filename, saved_as, status, total_commits, repos_json, job_id, error_msg, uploaded_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        upload.get("id"),
                        upload.get("filename"),
                        upload.get("saved_as"),
                        upload.get("status"),
                        upload.get("total_commits"),
                        json.dumps(upload.get("repos", [])),
                        upload.get("job_id"),
                        upload.get("error"),
                        upload.get("uploaded_at"),
                    ),
                )
        except Exception as e:
            logger.error(f"Failed to upsert upload {upload.get('id')}: {e}")

    def get_uploads(self) -> List[Dict]:
        try:
            with self._get_conn() as conn:
                rows = conn.execute(
                    """
                    SELECT id, filename, saved_as, status, total_commits, repos_json, job_id, error_msg, uploaded_at
                    FROM uploads
                    ORDER BY uploaded_at DESC
                    """
                ).fetchall()
            result = []
            for row in rows:
                result.append(
                    {
                        "id": row[0],
                        "filename": row[1],
                        "saved_as": row[2],
                        "status": row[3],
                        "total_commits": row[4] or 0,
                        "repos": json.loads(row[5] or "[]"),
                        "job_id": row[6],
                        "error": row[7],
                        "uploaded_at": row[8],
                    }
                )
            return result
        except Exception as e:
            logger.error(f"Failed to get uploads: {e}")
            return []

    def update_upload_status(
        self,
        upload_id: str,
        status: Optional[str] = None,
        job_id: Optional[str] = None,
        error: Optional[str] = None,
    ):
        try:
            with self._get_conn() as conn:
                current = conn.execute(
                    "SELECT filename, saved_as, total_commits, repos_json, uploaded_at FROM uploads WHERE id = ?",
                    (upload_id,),
                ).fetchone()
                if not current:
                    return
                conn.execute(
                    """
                    UPDATE uploads
                    SET status = COALESCE(?, status),
                        job_id = COALESCE(?, job_id),
                        error_msg = COALESCE(?, error_msg)
                    WHERE id = ?
                    """,
                    (status, job_id, error, upload_id),
                )
        except Exception as e:
            logger.error(f"Failed to update upload {upload_id}: {e}")
