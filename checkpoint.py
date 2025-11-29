import logging
import sqlite3
import time
from pathlib import Path
from typing import Dict, Optional

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
                        updated_at REAL
                    )
                    """
                )
        except Exception as e:
            logger.error(f"Failed to init DB: {e}")

    def reset_pending_jobs(self):
        """Clear leftover pending jobs from previous runs so they can be claimed again."""
        try:
            with self._get_conn() as conn:
                conn.execute("DELETE FROM scans WHERE status = 'PENDING'")
            logger.info("Reset pending jobs from previous run.")
        except Exception as e:
            logger.error(f"Failed to reset pending jobs: {e}")

    def try_claim_commit(self, commit_sha: str) -> bool:
        """
        Attempt to claim a commit for processing.
        Returns True if claimed; False if already present (pending/processed/failed).
        """
        try:
            with self._get_conn() as conn:
                conn.execute(
                    "INSERT INTO scans (commit_sha, status, updated_at) VALUES (?, 'PENDING', ?)",
                    (commit_sha, time.time()),
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

    def mark_processed(self, commit_sha: str):
        self._update_status(commit_sha, "PROCESSED")

    def mark_failed(self, commit_sha: str, error: str):
        self._update_status(commit_sha, "FAILED", error)

    def _update_status(self, commit_sha: str, status: str, error: Optional[str] = None):
        try:
            with self._get_conn() as conn:
                conn.execute(
                    """
                    UPDATE scans
                    SET status = ?, error_msg = ?, updated_at = ?
                    WHERE commit_sha = ?
                    """,
                    (status, error, time.time(), commit_sha),
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
