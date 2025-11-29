from __future__ import annotations

import logging
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, List

from pipeline.github_api import GitHubAPI, GitHubAPIError, GitHubRateLimitError

LOG = logging.getLogger("pipeline.commit_replay")


class MissingForkCommitError(RuntimeError):
    """Raised when a commit only present on a fork cannot be reconstructed."""

    def __init__(self, commit_sha: str, message: str) -> None:
        self.commit_sha = commit_sha
        super().__init__(message)


@dataclass
class ReplayCommit:
    sha: str
    patch: str
    message: str


@dataclass
class ReplayPlan:
    base_sha: str
    commits: List[ReplayCommit]


def build_replay_plan(
    *,
    github: GitHubAPI,
    repo_slug: str,
    target_sha: str,
    commit_exists: Callable[[str], bool],
    max_depth: int = 50,
) -> ReplayPlan:
    """Use the GitHub API to locate patches needed to recreate a fork-only history."""

    if commit_exists(target_sha):
        raise ValueError(f"Commit {target_sha} already exists, replay is unnecessary")

    missing_commits: List[ReplayCommit] = []
    current = target_sha
    depth = 0
    visited: set[str] = set()

    while True:
        depth += 1
        if depth > max_depth:
            raise MissingForkCommitError(
                target_sha,
                f"Exceeded parent traversal limit ({max_depth}) before finding a reachable ancestor",
            )
        try:
            payload = github.get_commit(repo_slug, current)
        except GitHubRateLimitError:
            raise
        except GitHubAPIError as exc:
            raise MissingForkCommitError(
                current,
                f"GitHub API error while loading commit {current}: {exc}",
            ) from exc
        parents = payload.get("parents") or []
        if len(parents) != 1:
            raise MissingForkCommitError(
                current,
                "Cannot replay commit with zero or multiple parents",
            )
        try:
            patch = github.get_commit_patch(repo_slug, current)
        except GitHubRateLimitError:
            raise
        except GitHubAPIError as exc:
            raise MissingForkCommitError(
                current,
                f"Failed to download patch for commit {current}: {exc}",
            ) from exc
        message = (payload.get("commit") or {}).get("message", "")
        missing_commits.append(ReplayCommit(sha=current, patch=patch, message=message))
        parent_sha = parents[0].get("sha")
        if not parent_sha:
            raise MissingForkCommitError(
                current,
                "Commit metadata missing parent SHA; cannot continue",
            )
        if commit_exists(parent_sha):
            missing_commits.reverse()
            LOG.info(
                "Replaying %d fork commits onto ancestor %s to reconstruct %s",
                len(missing_commits),
                parent_sha,
                target_sha,
            )
            return ReplayPlan(base_sha=parent_sha, commits=missing_commits)
        if parent_sha in visited:
            raise MissingForkCommitError(
                current,
                "Detected a parent traversal loop while searching for reachable ancestor",
            )
        visited.add(current)
        current = parent_sha


def apply_replay_plan(worktree: Path, plan: ReplayPlan) -> None:
    for commit in plan.commits:
        LOG.info("Applying fork-only patch %s", commit.sha)
        _apply_patch(worktree, commit.patch, commit.sha)


def _apply_patch(worktree: Path, patch_text: str, commit_sha: str) -> None:
    if not patch_text.strip():
        LOG.debug("Commit %s patch is empty; skipping", commit_sha)
        return
    completed = subprocess.run(
        ["git", "apply", "--allow-empty", "--whitespace=nowarn"],
        cwd=str(worktree),
        input=patch_text,
        text=True,
        capture_output=True,
    )
    if completed.returncode != 0:
        output = (completed.stdout or "") + (completed.stderr or "")
        raise MissingForkCommitError(
            commit_sha,
            f"Failed to apply patch for commit {commit_sha}: {output}",
        )
