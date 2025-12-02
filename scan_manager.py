import subprocess
import shutil
import logging
import pandas as pd
import requests
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from filelock import FileLock
from config import Config
from checkpoint import CheckpointManager
from pipeline.github_api import GitHubAPI
from pipeline.commit_replay import (
    build_replay_plan,
    apply_replay_plan,
    MissingForkCommitError,
)

# Setup logging
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class MiniScanner:
    def __init__(self):
        self.work_dir = Path(Config.WORK_DIR)
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.repos_dir = self.work_dir / "repos"
        self.repos_dir.mkdir(parents=True, exist_ok=True)
        self.temp_dir = self.work_dir / "temp"
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.locks_dir = self.work_dir / "locks"
        self.locks_dir.mkdir(parents=True, exist_ok=True)

        self.checkpoint = CheckpointManager(Config.CHECKPOINT_FILE)
        # Don't reset pending jobs - they will be resumed on restart
        # self.checkpoint.reset_pending_jobs()
        self.github = GitHubAPI(Config.GITHUB_TOKENS) if Config.GITHUB_TOKENS else None
        self.cleanup_stale_worktrees()

    def _get_repo_lock(self, repo_name):
        return FileLock(str(self.locks_dir / f"{repo_name}.lock"), timeout=600)

    def cleanup_stale_worktrees(self):
        """
        Remove leftover temp worktrees and prune git metadata from previous runs.
        """
        startup_lock = FileLock(str(self.locks_dir / "startup.lock"), timeout=60)
        with startup_lock:
            # Clean temp worktrees
            if self.temp_dir.exists():
                for child in self.temp_dir.iterdir():
                    try:
                        if child.is_dir():
                            shutil.rmtree(child, ignore_errors=True)
                        else:
                            child.unlink(missing_ok=True)
                    except Exception as e:
                        logger.warning(f"Failed to clean temp entry {child}: {e}")

            # Prune worktree metadata for each repo
            if self.repos_dir.exists():
                for repo_dir in self.repos_dir.iterdir():
                    if not repo_dir.is_dir():
                        continue
                    git_dir = repo_dir / ".git"
                    if not git_dir.exists():
                        continue
                    try:
                        with self._get_repo_lock(repo_dir.name):
                            self.run_command(
                                ["git", "worktree", "prune"], cwd=repo_dir, allow_fail=True
                            )
                    except Exception as e:
                        logger.warning(f"Failed to prune worktrees for {repo_dir}: {e}")

    def run_command(self, cmd, cwd=None, allow_fail=False):
        # logger.debug(f"Running command: {' '.join(cmd)}")
        try:
            result = subprocess.run(
                cmd,
                cwd=str(cwd) if cwd else None,
                capture_output=True,
                text=True,
                check=not allow_fail,
            )
            return result.stdout
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed: {e.cmd}")
            logger.error(f"Stdout: {e.stdout}")
            logger.error(f"Stderr: {e.stderr}")
            if not allow_fail:
                raise e
            return e.stdout + e.stderr

    def ensure_repo(self, repo_url, repo_name):
        """
        Ensures the 'master' copy of the repo exists in repos_dir.
        """
        repo_path = self.repos_dir / repo_name
        repo_lock = self._get_repo_lock(repo_name)
        with repo_lock:
            if not repo_path.exists():
                logger.info(f"Cloning {repo_url} to {repo_path}")
                self.run_command(["git", "clone", repo_url, str(repo_path)])
            else:
                # Single-threaded fetch to avoid collisions
                try:
                    self.run_command(
                        ["git", "fetch", "--all"], cwd=repo_path, allow_fail=True
                    )
                except Exception:
                    pass
        return repo_path
 
    def prepare_workspace(self, repo_name, project_key):
        """
        Create a git worktree for this job instead of cloning the whole repo.
        """
        master_repo_path = self.repos_dir / repo_name
        workspace_path = self.temp_dir / project_key

        if not master_repo_path.exists():
            raise RuntimeError(f"Master repo {repo_name} not prepared at {master_repo_path}")

        repo_lock = self._get_repo_lock(repo_name)
        with repo_lock:
            if workspace_path.exists():
                # Clean stale worktree metadata if any
                self.run_command(
                    ["git", "worktree", "remove", str(workspace_path), "--force"],
                    cwd=master_repo_path,
                    allow_fail=True,
                )
                shutil.rmtree(workspace_path, ignore_errors=True)

            # Create detached worktree at HEAD; specific commit checked out later
            self.run_command(
                ["git", "worktree", "add", "--detach", str(workspace_path), "HEAD"],
                cwd=master_repo_path,
            )

        return workspace_path

    def _commit_exists(self, repo_path, commit_sha):
        try:
            self.run_command(
                ["git", "cat-file", "-e", f"{commit_sha}^{{commit}}"], cwd=repo_path
            )
            return True
        except Exception:
            return False

    def checkout_commit(self, repo_path, commit_sha, repo_slug=None):
        logger.info(f"Checking out commit {commit_sha} in {repo_path}")

        # 1. Try standard checkout
        if self._commit_exists(repo_path, commit_sha):
            try:
                self.run_command(["git", "checkout", "-f", commit_sha], cwd=repo_path)
                self.run_command(["git", "clean", "-fdx"], cwd=repo_path)
                return
            except Exception as e:
                logger.warning(f"Standard checkout failed for {commit_sha}: {e}")

        # 2. If missing and we have GitHub tokens, try replay
        if self.github and repo_slug:
            logger.info(
                f"Commit {commit_sha} missing locally. Attempting replay from GitHub..."
            )
            try:
                plan = build_replay_plan(
                    github=self.github,
                    repo_slug=repo_slug,
                    target_sha=commit_sha,
                    commit_exists=lambda sha: self._commit_exists(repo_path, sha),
                )

                # Checkout base
                self.run_command(
                    ["git", "checkout", "-f", plan.base_sha], cwd=repo_path
                )
                self.run_command(["git", "clean", "-fdx"], cwd=repo_path)

                # Apply patches
                apply_replay_plan(repo_path, plan)
                logger.info(f"Successfully replayed commit {commit_sha}")
                return
            except MissingForkCommitError as e:
                logger.error(f"Failed to replay commit {commit_sha}: {e}")
                raise e
            except Exception as e:
                logger.error(f"Unexpected error during replay: {e}")
                raise e

        raise RuntimeError(f"Commit {commit_sha} not found and cannot be replayed.")

    def check_dependencies(self):
        # Check for git
        if shutil.which("git") is None:
            raise RuntimeError("git is not installed or not in PATH.")
        # Check for sonar-scanner
        if shutil.which(Config.SONAR_SCANNER_BIN) is None:
            logger.warning(
                f"'{Config.SONAR_SCANNER_BIN}' not found in PATH. Ensure it is installed and configured."
            )

    def run_sonar_scan(self, repo_path, project_key, commit_sha):
        logger.info(f"Starting SonarQube scan for {project_key} at {commit_sha}")

        cmd = [
            Config.SONAR_SCANNER_BIN,
            f"-Dsonar.projectKey={project_key}",
            f"-Dsonar.projectName={project_key}",
            f"-Dsonar.projectVersion={commit_sha}",
            "-Dsonar.sources=.",
            f"-Dsonar.host.url={Config.SONAR_HOST_URL}",
            f"-Dsonar.login={Config.SONAR_TOKEN}",
            "-Dsonar.scm.disabled=true",  # Disable SCM sensor to avoid issues with detached HEAD or shallow clones if any
            "-Dsonar.java.binaries=.",  # Assuming Java, but this might need adjustment for other languages
        ]
        if Config.SONAR_EXCLUSIONS and Config.SONAR_EXCLUSIONS.strip():
            cmd.append(f"-Dsonar.exclusions={Config.SONAR_EXCLUSIONS}")

        try:
            self.run_command(cmd, cwd=repo_path)
            logger.info(f"Scan completed successfully for {project_key}")
            if Config.WAIT_FOR_CE:
                self.wait_for_compute_engine(project_key)
            return True
        except Exception as e:
            logger.error(f"Scan failed for {project_key}: {e}")
            return False

    def wait_for_compute_engine(self, project_key: str):
        """
        Poll SonarQube Compute Engine until tasks for this project finish, to avoid fetching empty measures.
        """
        if not Config.SONAR_HOST_URL or not Config.SONAR_TOKEN:
            logger.warning("Cannot wait for Compute Engine (missing SONAR_HOST_URL or SONAR_TOKEN).")
            return

        deadline = time.time() + Config.WAIT_FOR_CE_TIMEOUT
        url = f"{Config.SONAR_HOST_URL.rstrip('/')}/api/ce/component"
        auth = (Config.SONAR_TOKEN, "")

        while time.time() < deadline:
            try:
                resp = requests.get(url, params={"component": project_key}, auth=auth, timeout=30)
                if resp.status_code == 401:
                    logger.warning("Unauthorized to query CE status; skipping wait.")
                    return
                resp.raise_for_status()
                data = resp.json()
                current = data.get("current")
                queue = data.get("queue", [])

                # If no current task and queue empty, we're done
                if not current and not queue:
                    logger.info(f"CE done for {project_key}")
                    return

                # If current exists, check status
                if current:
                    status = current.get("status")
                    task_id = current.get("id")
                    if status in {"SUCCESS", "FAILED", "CANCELED"}:
                        logger.info(f"CE task {task_id} for {project_key} finished with {status}")
                        if status == "SUCCESS":
                            return
                        else:
                            return
                logger.info(
                    f"Waiting for CE tasks of {project_key} "
                    f"(in queue: {len(queue)}, status: {current.get('status') if current else 'none'})"
                )
            except Exception as exc:
                logger.warning(f"Error polling CE for {project_key}: {exc}")

            time.sleep(Config.WAIT_FOR_CE_POLL)

        logger.warning(f"Timed out waiting for CE tasks for {project_key}")

    def process_single_job(self, row):
        # Support both column formats
        gh_project_name = row.get("gh_project_name")
        commit_sha = row.get("git_trigger_commit") or row.get("commit_sha")
        repo_url = row.get("repo_url")

        # If we have gh_project_name, construct repo_url
        if gh_project_name and not repo_url:
            repo_url = f"https://github.com/{gh_project_name}.git"

        if not repo_url or not commit_sha:
            logger.warning(f"Skipping row - missing repo_url or commit_sha: {row}")
            return False

        repo_slug = repo_url.split("github.com/")[-1].replace(".git", "") if "github.com" in repo_url else None
        owner = None
        repo_name = repo_url.split("/")[-1].replace(".git", "")
        if repo_slug and "/" in repo_slug:
            owner, repo_name = repo_slug.split("/", 1)

        # Default project_key now includes owner to avoid collisions across orgs
        project_key = row.get("project_key")
        if not project_key:
            if owner:
                project_key = f"{owner}_{repo_name}_{commit_sha}"
            else:
                project_key = f"{repo_name}_{commit_sha}"

        # Claim the commit to avoid duplicate processing across workers
        if not self.checkpoint.try_claim_commit(
            commit_sha,
            repo_name=repo_name,
            project_key=project_key,
            repo_url=repo_url,
        ):
            logger.debug(f"Skipping {project_key} (already processed or pending)")
            return True

        workspace_path = None
        try:
            # 1. Prepare workspace (worktree) from master repo prepared per batch
            workspace_path = self.prepare_workspace(repo_name, project_key)

            # 2. Checkout/Replay
            repo_slug = None
            if "github.com" in repo_url:
                parts = repo_url.split("github.com/")[-1].replace(".git", "").split("/")
                if len(parts) >= 2:
                    repo_slug = f"{parts[0]}/{parts[1]}"

            self.checkout_commit(workspace_path, commit_sha, repo_slug)

            # 4. Run Scan
            success = self.run_sonar_scan(workspace_path, project_key, commit_sha)

            if success:
                self.checkpoint.mark_processed(
                    commit_sha,
                    repo_name=repo_name,
                    project_key=project_key,
                    repo_url=repo_url,
                )
                return True
            else:
                self.checkpoint.mark_failed(
                    commit_sha,
                    "Scanner command failed",
                    repo_name=repo_name,
                    project_key=project_key,
                    repo_url=repo_url,
                )
                return False

        except Exception as e:
            logger.error(f"Failed to process {project_key}: {e}")
            self.checkpoint.mark_failed(
                commit_sha,
                str(e),
                repo_name=repo_name,
                project_key=project_key,
                repo_url=repo_url,
            )
            return False
        finally:
            # Cleanup workspace
            if workspace_path and workspace_path.exists():
                repo_lock = self._get_repo_lock(repo_name)
                with repo_lock:
                    try:
                        self.run_command(
                            ["git", "worktree", "remove", str(workspace_path), "--force"],
                            cwd=self.repos_dir / repo_name,
                            allow_fail=True,
                        )
                    except Exception as e:
                        logger.warning(f"Failed to remove worktree {workspace_path}: {e}")
                shutil.rmtree(workspace_path, ignore_errors=True)

    def process_csv(self, csv_path, batch_size=Config.BATCH_SIZE):
        logger.info(f"Processing {csv_path} in batches of {batch_size}")

        try:
            futures = []
            with ThreadPoolExecutor(max_workers=Config.CONCURRENT_SCANS) as executor:
                for batch_idx, df_chunk in enumerate(
                    pd.read_csv(csv_path, chunksize=batch_size)
                ):
                    logger.info(f"--- Starting Batch {batch_idx + 1} ---")
                    logger.debug(f"Batch columns: {df_chunk.columns.tolist()}")
                    rows = df_chunk.to_dict("records")
                    logger.debug(f"Number of rows in batch: {len(rows)}")
                    if rows:
                        logger.debug(f"First row: {rows[0]}")

                    # Pre-ensure repos for this batch to avoid concurrent clones
                    repos_to_prepare = {}
                    for row in rows:
                        gh_project_name = row.get("gh_project_name")
                        repo_url = row.get("repo_url")
                        if gh_project_name and not repo_url:
                            repo_url = f"https://github.com/{gh_project_name}.git"
                        if not repo_url:
                            continue
                        repo_name = repo_url.split("/")[-1].replace(".git", "")
                        repos_to_prepare[repo_url] = repo_name

                    for repo_url, repo_name in repos_to_prepare.items():
                        try:
                            self.ensure_repo(repo_url, repo_name)
                        except Exception as e:
                            logger.error(
                                f"Failed to prepare repo {repo_name} ({repo_url}): {e}"
                            )

                    for row in rows:
                        futures.append(executor.submit(self.process_single_job, row))

                    logger.info(f"--- Scheduled Batch {batch_idx + 1} ({len(rows)} jobs) ---")

                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error(f"Job failed with exception: {e}")

            logger.info("--- All batches scheduled and completed ---")
        except Exception as e:
            logger.error(f"Failed to process CSV {csv_path}: {e}")
