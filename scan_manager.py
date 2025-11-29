import subprocess
import shutil
import logging
import pandas as pd
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from config import Config
from checkpoint import CheckpointManager
from pipeline.github_api import GitHubAPI
from pipeline.commit_replay import build_replay_plan, apply_replay_plan, MissingForkCommitError

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MiniScanner:
    def __init__(self):
        self.work_dir = Path(Config.WORK_DIR)
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.repos_dir = self.work_dir / "repos"
        self.repos_dir.mkdir(parents=True, exist_ok=True)
        self.temp_dir = self.work_dir / "temp"
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        
        self.checkpoint = CheckpointManager(Config.CHECKPOINT_FILE)
        self.github = GitHubAPI(Config.GITHUB_TOKENS) if Config.GITHUB_TOKENS else None

    def run_command(self, cmd, cwd=None, allow_fail=False):
        # logger.debug(f"Running command: {' '.join(cmd)}")
        try:
            result = subprocess.run(
                cmd,
                cwd=str(cwd) if cwd else None,
                capture_output=True,
                text=True,
                check=not allow_fail
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
        if not repo_path.exists():
            logger.info(f"Cloning {repo_url} to {repo_path}")
            self.run_command(["git", "clone", repo_url, str(repo_path)])
        else:
            # logger.info(f"Repo {repo_name} exists. Fetching updates...")
            # We can skip fetch if we assume we have what we need, or fetch periodically.
            # For concurrency, multiple threads might try to fetch same repo. 
            # Ideally we should lock this, but git handles concurrent fetches somewhat okay, 
            # or we can just ignore errors if it's busy.
            # For now, let's assume we fetch once or just try.
            try:
                self.run_command(["git", "fetch", "--all"], cwd=repo_path, allow_fail=True)
            except Exception:
                pass
        return repo_path

    def prepare_workspace(self, repo_name, project_key):
        """
        Creates a temporary clone for the specific scan job.
        """
        master_repo_path = self.repos_dir / repo_name
        workspace_path = self.temp_dir / project_key
        
        if workspace_path.exists():
            shutil.rmtree(workspace_path)
            
        # Clone from local master repo to temp workspace
        # Using file:// protocol to clone locally
        self.run_command(["git", "clone", str(master_repo_path), str(workspace_path)])
        return workspace_path

    def _commit_exists(self, repo_path, commit_sha):
        try:
            self.run_command(["git", "cat-file", "-e", f"{commit_sha}^{{commit}}"], cwd=repo_path)
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
            logger.info(f"Commit {commit_sha} missing locally. Attempting replay from GitHub...")
            try:
                plan = build_replay_plan(
                    github=self.github,
                    repo_slug=repo_slug,
                    target_sha=commit_sha,
                    commit_exists=lambda sha: self._commit_exists(repo_path, sha)
                )
                
                # Checkout base
                self.run_command(["git", "checkout", "-f", plan.base_sha], cwd=repo_path)
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
             logger.warning(f"'{Config.SONAR_SCANNER_BIN}' not found in PATH. Ensure it is installed and configured.")

    def run_sonar_scan(self, repo_path, project_key, commit_sha):
        logger.info(f"Starting SonarQube scan for {project_key} at {commit_sha}")
        
        cmd = [
            Config.SONAR_SCANNER_BIN,
            f"-Dsonar.projectKey={project_key}",
            f"-Dsonar.projectName={project_key}",
            f"-Dsonar.projectVersion={commit_sha}",
            "-Dsonar.sources=.",
            f"-Dsonar.host.url={Config.SONAR_HOST_URL}",
            f"-Dsonar.token={Config.SONAR_TOKEN}",
            "-Dsonar.scm.disabled=true", # Disable SCM sensor to avoid issues with detached HEAD or shallow clones if any
            "-Dsonar.java.binaries=." # Assuming Java, but this might need adjustment for other languages
        ]

        try:
            self.run_command(cmd, cwd=repo_path)
            logger.info(f"Scan completed successfully for {project_key}")
            return True
        except Exception as e:
            logger.error(f"Scan failed for {project_key}: {e}")
            return False

    def process_single_job(self, row):
        repo_url = row.get('repo_url')
        commit_sha = row.get('commit_sha')
        
        if not repo_url or not commit_sha:
            return False

        repo_name = repo_url.split('/')[-1].replace('.git', '')
        project_key = row.get('project_key', f"{repo_name}_{commit_sha}")

        if self.checkpoint.is_processed(commit_sha):
            logger.info(f"Skipping {project_key} (already processed)")
            return True

        workspace_path = None
        try:
            # 1. Ensure master repo exists (thread-safe enough if we just fetch)
            # Note: ensure_repo might have race conditions if multiple threads try to clone same repo at once.
            # Ideally we should do this sequentially before scanning, or use a lock.
            # For now, let's assume the user pre-clones or we handle it.
            # We can add a lock per repo_name if needed, but let's keep it simple.
            self.ensure_repo(repo_url, repo_name)

            # 2. Prepare workspace
            workspace_path = self.prepare_workspace(repo_name, project_key)

            # 3. Checkout/Replay
            repo_slug = None
            if "github.com" in repo_url:
                parts = repo_url.split("github.com/")[-1].replace(".git", "").split("/")
                if len(parts) >= 2:
                    repo_slug = f"{parts[0]}/{parts[1]}"

            self.checkout_commit(workspace_path, commit_sha, repo_slug)

            # 4. Run Scan
            success = self.run_sonar_scan(workspace_path, project_key, commit_sha)
            
            if success:
                self.checkpoint.mark_processed(commit_sha)
                return True
            else:
                self.checkpoint.mark_failed(commit_sha, "Scanner command failed")
                return False

        except Exception as e:
            logger.error(f"Failed to process {project_key}: {e}")
            self.checkpoint.mark_failed(commit_sha, str(e))
            return False
        finally:
            # Cleanup workspace
            if workspace_path and workspace_path.exists():
                try:
                    shutil.rmtree(workspace_path)
                except Exception as e:
                    logger.warning(f"Failed to cleanup workspace {workspace_path}: {e}")

    def process_csv(self, csv_path, batch_size=Config.BATCH_SIZE):
        logger.info(f"Processing {csv_path} in batches of {batch_size}")
        
        try:
            for batch_idx, df_chunk in enumerate(pd.read_csv(csv_path, chunksize=batch_size)):
                logger.info(f"--- Starting Batch {batch_idx + 1} ---")
                rows = df_chunk.to_dict('records')
                
                with ThreadPoolExecutor(max_workers=Config.CONCURRENT_SCANS) as executor:
                    futures = {executor.submit(self.process_single_job, row): row for row in rows}
                    
                    for future in as_completed(futures):
                        try:
                            future.result()
                        except Exception as e:
                            logger.error(f"Job failed with exception: {e}")
                
                logger.info(f"--- Completed Batch {batch_idx + 1} ---")
        except Exception as e:
            logger.error(f"Failed to process CSV {csv_path}: {e}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Mini SonarQube Scanner Pipeline (Batch)")
    parser.add_argument("csv_file", help="Path to the CSV file containing repo and commit info")
    args = parser.parse_args()

    scanner = MiniScanner()
    scanner.check_dependencies()
    scanner.process_csv(args.csv_file)
