import os
import time
import datetime
import requests
from typing import Dict, Any, List, Tuple

GELBOORU_API = "https://gelbooru.com/index.php"
PAGE_LIMIT = 100
MAX_PAGES_PER_CHECK = 5
DEFAULT_ANTI_BAN_PAUSE = 3.0


def _parse_created_at(created_at_str: str) -> float:
    try:
        return datetime.datetime.strptime(
            created_at_str or "", "%a %b %d %H:%M:%S %z %Y"
        ).timestamp()
    except Exception:
        return 0.0


def _build_gelbooru_query(tags: List[str], ratings: List[str]) -> str:
    parts = list(tags)
    for r in (ratings or []):
        r_str = str(r).strip()
        if not r_str.startswith("rating:"):
            parts.append(f"rating:{r_str}")
        else:
            parts.append(r_str)
    return " ".join(parts)


def check_gelbooru(watcher: Dict[str, Any], store, proxy_url: str = None) -> Dict[str, Any]:
    """Execute a single bounded check on Gelbooru for new posts."""
    wid = watcher["watcher_id"]
    tags = watcher.get("tags", [])
    ratings = watcher.get("ratings", [])
    query_str = _build_gelbooru_query(tags, ratings)

    proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None
    anti_ban_pause = float(os.getenv("ANTI_BAN_PAUSE", str(DEFAULT_ANTI_BAN_PAUSE)))

    api_key = os.getenv("GELBOORU_API_KEY", "")
    user_id = os.getenv("GELBOORU_USER_ID", "")

    headers = {"User-Agent": "RemsWatcher/1.0"}
    pid = int(watcher.get("scan_pid") or 0)
    checkpoint = watcher.get("last_seen_post_id")
    cycle_newest = watcher.get("scan_newest_id")

    fresh = []
    reached = False
    exhausted = False
    pages_done = 0

    try:
        def fetch_page(page_num: int):
            params = {
                "page": "dapi",
                "s": "post",
                "q": "index",
                "tags": query_str,
                "pid": page_num,
                "limit": PAGE_LIMIT,
                "json": 1
            }
            if api_key and user_id:
                params["api_key"] = api_key
                params["user_id"] = user_id

            resp = requests.get(GELBOORU_API, params=params, headers=headers, proxies=proxies, timeout=20)
            if resp.status_code in (401, 403, 429):
                raise RuntimeError(f"Gelbooru API rate limit or auth error: HTTP {resp.status_code}")
            resp.raise_for_status()
            data = resp.json() or {}
            posts = data.get("post") or []
            return posts if isinstance(posts, list) else [posts]

        # First page scan
        posts = fetch_page(pid)
        pages_done += 1

        if not posts:
            store.set_runtime(wid, {"last_checked_at": time.time(), "error": ""})
            return {"new": 0, "error": ""}

        # If not initialized, set baseline to current newest post and only notify posts newer than creation
        if not watcher.get("initialized"):
            cutoff = float(watcher.get("baseline_at") or 0)
            fresh_items = [p for p in posts if cutoff > 0 and _parse_created_at(p.get("created_at")) >= cutoff]
            entries = []
            for p in fresh_items:
                entries.append({
                    "watcher_id": wid,
                    "external_id": str(p.get("id")),
                    "title": f"Gelbooru: {', '.join(tags)}",
                    "thumb": p.get("preview_url") or p.get("sample_url") or "",
                    "rating": p.get("rating", ""),
                    "url": p.get("file_url") or "",
                    "metadata": {"tags": tags, "post_id": str(p.get("id"))}
                })

            new_entries = store.ingest_notifications("gelbooru", entries) if entries else []
            store.set_runtime(wid, {
                "initialized": True,
                "last_seen_post_id": str(posts[0]["id"]),
                "last_checked_at": time.time(),
                "error": ""
            })
            return {"new": len(new_entries), "error": ""}

        cycle_newest = str(posts[0]["id"])
        for p in posts:
            if str(p.get("id")) == str(checkpoint):
                reached = True
                break
            fresh.append(p)

        # Pagination loop if checkpoint was not reached on page 0
        pid = 1
        while not reached and not exhausted and pages_done < MAX_PAGES_PER_CHECK:
            time.sleep(anti_ban_pause)
            posts = fetch_page(pid)
            pages_done += 1
            if not posts:
                exhausted = True
                break

            for p in posts:
                if str(p.get("id")) == str(checkpoint):
                    reached = True
                    break
                fresh.append(p)
            pid += 1

        # Prepare notifications for all fresh posts
        entries = []
        for p in fresh:
            entries.append({
                "watcher_id": wid,
                "external_id": str(p.get("id")),
                "title": f"Gelbooru: {', '.join(tags)}",
                "thumb": p.get("preview_url") or p.get("sample_url") or "",
                "rating": p.get("rating", ""),
                "url": p.get("file_url") or "",
                "metadata": {"tags": tags, "post_id": str(p.get("id"))}
            })

        new_entries = store.ingest_notifications("gelbooru", entries) if entries else []

        if reached or exhausted:
            store.set_runtime(wid, {
                "last_seen_post_id": cycle_newest or checkpoint,
                "initialized": True,
                "scan_pid": 0,
                "scan_newest_id": None,
                "last_checked_at": time.time(),
                "error": ""
            })
            return {"new": len(new_entries), "error": ""}
        else:
            # Reached max page cap for this run; resume deeper on next run
            store.set_runtime(wid, {
                "scan_pid": pid,
                "scan_newest_id": cycle_newest,
                "last_checked_at": time.time(),
                "error": "Batch page limit reached; will continue next cycle."
            })
            return {"new": len(new_entries), "error": ""}

    except Exception as e:
        err_msg = str(e)[:250]
        store.set_runtime(wid, {"last_checked_at": time.time(), "error": err_msg})
        return {"new": 0, "error": err_msg}
