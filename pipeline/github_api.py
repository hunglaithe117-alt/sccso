from __future__ import annotations

import logging
import threading
import time
from typing import List, Optional
import requests
from urllib.parse import quote

LOG = logging.getLogger("pipeline.github")

class AllTokensRateLimited(RuntimeError):
    def __init__(self, retry_at: float) -> None:
        self.retry_at = retry_at
        human = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(retry_at))
        super().__init__(f"All GitHub tokens are rate limited until {human} UTC")

class GitHubAPIError(RuntimeError):
    def __init__(self, status_code: int, message: str) -> None:
        self.status_code = status_code
        super().__init__(message)

class GitHubRateLimitError(GitHubAPIError):
    def __init__(self, retry_at: float, message: str) -> None:
        super().__init__(403, message)
        self.retry_at = retry_at

class GitHubTokenPool:
    def __init__(self, tokens: List[str]) -> None:
        cleaned = [token.strip() for token in tokens if token and token.strip()]
        if not cleaned:
            # Fallback or empty, but better to have tokens
            self.tokens = []
        else:
            self.tokens = cleaned
        self._cooldowns: dict[str, float] = {token: 0.0 for token in self.tokens}
        self._cursor = 0
        self._lock = threading.Lock()

    def acquire(self) -> str:
        if not self.tokens:
            raise RuntimeError("No GitHub tokens configured.")
        
        now = time.time()
        with self._lock:
            for _ in range(len(self.tokens)):
                token = self.tokens[self._cursor]
                self._cursor = (self._cursor + 1) % len(self.tokens)
                if self._cooldowns.get(token, 0.0) <= now:
                    return token
        raise AllTokensRateLimited(self.next_available_at())

    def mark_rate_limited(self, token: str, reset_epoch: Optional[int]) -> None:
        if token not in self._cooldowns:
            return
        cooldown = float(reset_epoch) if reset_epoch else time.time() + 60.0
        with self._lock:
            self._cooldowns[token] = max(cooldown, time.time() + 1.0)
        reset_text = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(cooldown))
        LOG.warning("GitHub token exhausted, cooling down until %s", reset_text)

    def next_available_at(self) -> float:
        if not self._cooldowns:
            return time.time()
        return min(self._cooldowns.values())

class GitHubAPI:
    def __init__(self, tokens: List[str], timeout: int = 30) -> None:
        self.base_url = "https://api.github.com"
        self.token_pool = GitHubTokenPool(tokens)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "mini-scan-pipeline",
        })
        self.timeout = timeout

    def _request(
        self,
        method: str,
        path: str,
        *,
        accept: str,
        params: Optional[dict[str, str]] = None,
    ) -> requests.Response:
        url = f"{self.base_url}{path}"
        
        # Simple retry logic
        for _ in range(3):
            try:
                token = self.token_pool.acquire()
            except AllTokensRateLimited as e:
                # Wait or fail? For now, fail or wait briefly?
                # Let's just raise to let caller handle or fail job
                raise e

            headers = {
                "Accept": accept,
                "Authorization": f"token {token}",
            }
            
            try:
                resp = self.session.request(
                    method,
                    url,
                    params=params,
                    headers=headers,
                    timeout=self.timeout,
                )
                
                if resp.status_code == 403 and resp.headers.get("X-RateLimit-Remaining") == "0":
                    reset = int(resp.headers.get("X-RateLimit-Reset", 0))
                    self.token_pool.mark_rate_limited(token, reset)
                    continue
                
                if resp.status_code >= 400:
                    # If 404, maybe return? But let's raise for now
                    pass
                    
                return resp
            except requests.RequestException:
                continue
        
        raise GitHubAPIError(500, "Failed to connect to GitHub API after retries")

    def _encode_slug(self, repo_slug: str) -> str:
        return quote(repo_slug, safe="/")

    def get_commit(self, repo_slug: str, commit_sha: str) -> dict:
        slug = self._encode_slug(repo_slug)
        resp = self._request(
            "GET",
            f"/repos/{slug}/commits/{commit_sha}",
            accept="application/vnd.github+json",
        )
        if resp.status_code != 200:
            raise GitHubAPIError(resp.status_code, resp.text)
        return resp.json()

    def get_commit_patch(self, repo_slug: str, commit_sha: str) -> str:
        slug = self._encode_slug(repo_slug)
        resp = self._request(
            "GET",
            f"/repos/{slug}/commits/{commit_sha}",
            accept="application/vnd.github.v3.patch",
        )
        if resp.status_code != 200:
            raise GitHubAPIError(resp.status_code, resp.text)
        return resp.text
