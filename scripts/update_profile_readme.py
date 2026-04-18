#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

OWNER = os.environ.get("GITHUB_REPOSITORY_OWNER") or os.environ.get("OWNER")
REPOSITORY = os.environ.get("GITHUB_REPOSITORY", "")
REPO_NAME = REPOSITORY.split("/", 1)[-1] if "/" in REPOSITORY else None
TOKEN = os.environ.get("GITHUB_TOKEN", "")
README_PATH = Path(os.environ.get("README_PATH", "README.md"))

# How many repos to show in the "popular" section
TOP_N = int(os.environ.get("TOP_N", "5"))

# Profile repo is usually named exactly like the username - exclude it
PROFILE_REPO_NAME: str = REPO_NAME or OWNER or ""

if not OWNER:
    raise SystemExit("Missing GITHUB_REPOSITORY_OWNER (or OWNER).")

if not TOKEN:
    raise SystemExit("GITHUB_TOKEN is required for the GraphQL API (pinned repos).")


# ---------------------------------------------------------------------------
# GitHub API helpers
# ---------------------------------------------------------------------------


def _check_rate_limit(headers: Any) -> None:
    remaining = headers.get("X-RateLimit-Remaining", "?")
    reset_ts = headers.get("X-RateLimit-Reset", "")
    if remaining != "?" and int(remaining) < 10:
        reset_dt = (
            datetime.fromtimestamp(int(reset_ts), tz=timezone.utc).isoformat()
            if str(reset_ts).isdigit()
            else "unknown"
        )
        print(
            f"[warn] Only {remaining} GitHub API requests remaining "
            f"(resets at {reset_dt})"
        )


def gh_get(url: str) -> Any:
    """REST GET request."""
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "profile-readme-updater",
        "Authorization": f"Bearer {TOKEN}",
    }
    req = Request(url, headers=headers)
    try:
        with urlopen(req, timeout=30) as resp:
            _check_rate_limit(resp.headers)
            return json.loads(resp.read().decode("utf-8"))
    except HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        reset_ts = e.headers.get("X-RateLimit-Reset", "")
        if e.code in (403, 429) and str(reset_ts).isdigit():
            reset_dt = datetime.fromtimestamp(
                int(reset_ts), tz=timezone.utc
            ).isoformat()
            raise SystemExit(
                f"GitHub API rate limit hit (HTTP {e.code}). "
                f"Resets at {reset_dt}. Body: {body}"
            )
        raise SystemExit(f"GitHub API error {e.code} for {url}: {body}")
    except URLError as e:
        raise SystemExit(f"Network error for {url}: {e}")


def gh_graphql(query: str) -> Any:
    """GraphQL POST request."""
    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json",
        "User-Agent": "profile-readme-updater",
    }
    payload = json.dumps({"query": query}).encode()
    req = Request("https://api.github.com/graphql", data=payload, headers=headers)
    try:
        with urlopen(req, timeout=30) as resp:
            _check_rate_limit(resp.headers)
            data = json.loads(resp.read().decode("utf-8"))
    except HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise SystemExit(f"GitHub GraphQL error {e.code}: {body}")
    except URLError as e:
        raise SystemExit(f"Network error (GraphQL): {e}")

    if "errors" in data:
        raise SystemExit(f"GraphQL errors: {data['errors']}")

    return data


def parse_dt(value: Optional[str]) -> datetime:
    if not value:
        return datetime.fromtimestamp(0, tz=timezone.utc)
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------


def fetch_pinned_repos() -> List[Dict[str, Any]]:
    """
    Returns the repos the owner manually pinned on their profile page
    via GraphQL (up to 6, in pinned order).
    """
    query = (
        """
    {
      user(login: "%s") {
        pinnedItems(first: 6, types: REPOSITORY) {
          nodes {
            ... on Repository {
              name
              description
              url
              stargazerCount
              forkCount
              primaryLanguage { name }
              pushedAt
            }
          }
        }
      }
    }
    """
        % OWNER
    )

    data = gh_graphql(query)
    nodes = data.get("data", {}).get("user", {}).get("pinnedItems", {}).get("nodes", [])

    result = []
    for n in nodes:
        if not n:
            continue
        result.append(
            {
                "name": n.get("name", ""),
                "html_url": n.get("url", ""),
                "description": n.get("description") or "",
                "stargazers_count": n.get("stargazerCount", 0),
                "forks_count": n.get("forkCount", 0),
                "language": (n.get("primaryLanguage") or {}).get("name") or "",
                "pushed_at": n.get("pushedAt"),
            }
        )
    return result


def fetch_repos() -> List[Dict[str, Any]]:
    """
    Returns all public, non-fork repos owned by OWNER via REST.
    Used for the "popular" section.
    """
    repos: List[Dict[str, Any]] = []
    page = 1

    while True:
        params = {
            "type": "owner",
            "sort": "updated",
            "direction": "desc",
            "per_page": 100,
            "page": page,
        }
        url = f"https://api.github.com/users/{OWNER}/repos?{urlencode(params)}"
        batch = gh_get(url)

        if not isinstance(batch, list):
            raise SystemExit(
                "Unexpected GitHub API response shape while fetching repositories."
            )

        repos.extend(batch)
        if len(batch) < 100:
            break
        page += 1

    return repos


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------


def should_exclude(repo: Dict[str, Any]) -> bool:
    return (
        repo.get("archived", False)
        or repo.get("fork", False)
        or repo.get("disabled", False)
        or repo.get("name", "").lower() == PROFILE_REPO_NAME.lower()
    )


# ---------------------------------------------------------------------------
# Table rendering
# ---------------------------------------------------------------------------


def _lang(lang: str) -> str:
    return f"`{lang}`" if lang else "-"


def _stars(count: int) -> str:
    return f"★ {count}" if count else "-"


def _forks(count: int) -> str:
    return f"⑂ {count}" if count else "-"


def render_pinned_table(repos: List[Dict[str, Any]]) -> str:
    """
    | Project | Description | Language |
    Sourced from pinned repos - reflects what the owner is actively working on.
    """
    if not repos:
        return "_No pinned repositories. Pin some on your GitHub profile page._"

    rows = []
    for r in repos:
        name = r.get("name", "unknown")
        url = r.get("html_url", "")
        desc = (r.get("description") or "").strip() or "-"
        lang = _lang(r.get("language") or "")
        rows.append(f"| [`{name}`]({url}) | {desc} | {lang} |")

    return "\n".join(
        [
            "| Project | Description | Language |",
            "| ------- | ----------- | -------- |",
            *rows,
        ]
    )


def render_popular_table(repos: List[Dict[str, Any]]) -> str:
    """
    | Project | Description | Language | Stars | Forks |
    Sorted by stars descending, then by most recently pushed.
    """
    candidates = [r for r in repos if not should_exclude(r)]
    candidates.sort(
        key=lambda r: (
            int(r.get("stargazers_count", 0)),
            parse_dt(r.get("pushed_at")),
        ),
        reverse=True,
    )
    top = candidates[:TOP_N]

    if not top:
        return "_No public repositories to show yet._"

    rows = []
    for r in top:
        name = r.get("name", "unknown")
        url = r.get("html_url", "")
        desc = (r.get("description") or "").strip() or "-"
        lang = _lang(r.get("language") or "")
        stars = _stars(r.get("stargazers_count", 0))
        forks = _forks(r.get("forks_count", 0))
        rows.append(f"| [`{name}`]({url}) | {desc} | {lang} | {stars} | {forks} |")

    return "\n".join(
        [
            "| Project | Description | Language | Stars | Forks |",
            "| ------- | ----------- | -------- | ----- | ----- |",
            *rows,
        ]
    )


# ---------------------------------------------------------------------------
# README manipulation
# ---------------------------------------------------------------------------


def replace_block(text: str, block_name: str, content: str) -> str:
    start = f"<!-- AUTO: {block_name}:START -->"
    end = f"<!-- AUTO: {block_name}:END -->"
    pattern = re.compile(re.escape(start) + r".*?" + re.escape(end), re.DOTALL)

    if not pattern.search(text):
        raise SystemExit(
            f"Markers for block {block_name!r} not found in {README_PATH}. "
            f"Add <!-- AUTO: {block_name}:START --> and <!-- AUTO: {block_name}:END -->."
        )

    return pattern.sub(f"{start}\n{content}\n{end}", text, count=1)


def write_atomic(path: Path, text: str) -> None:
    """Write *text* to *path* atomically via a temp file in the same directory."""
    fd, tmp_path = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(text)
        shutil.move(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    if not README_PATH.exists():
        raise SystemExit(f"README not found: {README_PATH}")

    original = README_PATH.read_text(encoding="utf-8")

    print("Fetching pinned repos …")
    pinned = fetch_pinned_repos()
    print(f"  → {len(pinned)} pinned repo(s).")

    print("Fetching all public repos …")
    all_repos = fetch_repos()
    print(f"  → {len(all_repos)} repo(s) total.")

    updated = original
    updated = replace_block(updated, "active-repos", render_pinned_table(pinned))
    updated = replace_block(updated, "top-repos", render_popular_table(all_repos))

    if updated == original:
        print("README is already up to date - nothing to write.")
        return

    write_atomic(README_PATH, updated)
    print("README updated.")


if __name__ == "__main__":
    main()
