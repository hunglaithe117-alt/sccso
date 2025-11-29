import json
import os
import logging
from typing import Set, Dict

logger = logging.getLogger(__name__)

class CheckpointManager:
    def __init__(self, checkpoint_file: str):
        self.checkpoint_file = checkpoint_file
        self.processed_commits: Set[str] = set()
        self.failed_commits: Dict[str, str] = {}
        self.load()

    def load(self):
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    data = json.load(f)
                    self.processed_commits = set(data.get("processed", []))
                    self.failed_commits = data.get("failed", {})
                logger.info(f"Loaded checkpoint: {len(self.processed_commits)} processed, {len(self.failed_commits)} failed.")
            except Exception as e:
                logger.error(f"Failed to load checkpoint: {e}")
        else:
            logger.info("No checkpoint file found. Starting fresh.")

    def save(self):
        try:
            data = {
                "processed": list(self.processed_commits),
                "failed": self.failed_commits
            }
            with open(self.checkpoint_file, 'w') as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save checkpoint: {e}")

    def is_processed(self, commit_sha: str) -> bool:
        return commit_sha in self.processed_commits

    def mark_processed(self, commit_sha: str):
        self.processed_commits.add(commit_sha)
        if commit_sha in self.failed_commits:
            del self.failed_commits[commit_sha]
        self.save()

    def mark_failed(self, commit_sha: str, error: str):
        self.failed_commits[commit_sha] = error
        self.save()
