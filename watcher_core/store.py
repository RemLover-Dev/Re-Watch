import os
import json
import time
import uuid
import tempfile
import threading
from typing import Dict, Any, List, Tuple, Optional

_lock = threading.RLock()


def _atomic_save_json(filepath: str, data: Any):
    """Save data to JSON file atomically to prevent file corruption."""
    os.makedirs(os.path.dirname(os.path.abspath(filepath)), exist_ok=True)
    dir_name = os.path.dirname(os.path.abspath(filepath))
    with tempfile.NamedTemporaryFile("w", dir=dir_name, delete=False, encoding="utf-8") as tf:
        temp_name = tf.name
        json.dump(data, tf, indent=2, ensure_ascii=False)
    os.replace(temp_name, filepath)


def _load_json(filepath: str, default: Any = None):
    if not os.path.exists(filepath):
        return default if default is not None else {}
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default if default is not None else {}


class WatcherStore:
    """Manages persistence for watchers and notifications."""

    def __init__(self, data_dir: str):
        self.data_dir = os.path.abspath(data_dir)
        os.makedirs(self.data_dir, exist_ok=True)
        self.watchers_file = os.path.join(self.data_dir, "watchers.json")
        self.notifs_file = os.path.join(self.data_dir, "notifications.json")

    # --- Watchers CRUD ---
    def list_watchers(self, source: Optional[str] = None) -> List[Dict[str, Any]]:
        with _lock:
            raw = _load_json(self.watchers_file, {"watchers": []})
            ws = raw.get("watchers", [])
            if source:
                return [dict(w) for w in ws if w.get("source") == source]
            return [dict(w) for w in ws]

    def create_watcher(self, source: str, tags: List[str], ratings: Optional[List[str]] = None,
                       interval_minutes: int = 30) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        tags_clean = [str(t).strip().lower() for t in tags if str(t).strip()]
        if not tags_clean:
            return None, "At least one tag is required"

        w = {
            "watcher_id": "w_" + uuid.uuid4().hex[:8],
            "source": source.lower().strip(),
            "tags": tags_clean,
            "ratings": ratings or [],
            "interval_minutes": max(5, min(1440, int(interval_minutes))),
            "enabled": True,
            "initialized": False,
            "baseline_at": time.time(),
            "last_seen_post_id": None,
            "last_checked_at": 0,
            "scan_pid": 0,
            "scan_newest_id": None,
            "error": "",
            "created_at": time.time()
        }

        with _lock:
            raw = _load_json(self.watchers_file, {"watchers": []})
            raw.setdefault("watchers", []).append(w)
            _atomic_save_json(self.watchers_file, raw)
        return dict(w), None

    def update_watcher(self, wid: str, fields: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        with _lock:
            raw = _load_json(self.watchers_file, {"watchers": []})
            ws = raw.get("watchers", [])
            w = next((x for x in ws if x.get("watcher_id") == wid), None)
            if not w:
                return None, "Watcher not found"

            config_changed = any(k in fields and fields[k] != w.get(k) for k in ("tags", "ratings"))
            w.update(fields)
            if config_changed:
                w.update({
                    "initialized": False,
                    "last_seen_post_id": None,
                    "baseline_at": time.time(),
                    "scan_pid": 0,
                    "scan_newest_id": None,
                    "error": ""
                })

            _atomic_save_json(self.watchers_file, raw)
            return dict(w), None

    def set_runtime(self, wid: str, fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        with _lock:
            raw = _load_json(self.watchers_file, {"watchers": []})
            for w in raw.get("watchers", []):
                if w.get("watcher_id") == wid:
                    w.update(fields)
                    _atomic_save_json(self.watchers_file, raw)
                    return dict(w)
        return None

    def remove_watcher(self, wid: str) -> bool:
        with _lock:
            raw = _load_json(self.watchers_file, {"watchers": []})
            ws = raw.get("watchers", [])
            keep = [w for w in ws if w.get("watcher_id") != wid]
            if len(keep) == len(ws):
                return False
            raw["watchers"] = keep
            _atomic_save_json(self.watchers_file, raw)
        return True

    # --- Notifications Store ---
    def ingest_notifications(self, source: str, entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Store new notifications, deduplicating by source:watcher_id:external_id."""
        with _lock:
            raw = _load_json(self.notifs_file, {"notifications": []})
            existing_notifs = raw.get("notifications", [])
            seen_keys = {
                f"{n.get('source')}:{n.get('watcher_id')}:{n.get('external_id')}"
                for n in existing_notifs
            }

            added = []
            now = time.time()
            for entry in entries:
                key = f"{source}:{entry.get('watcher_id')}:{entry.get('external_id')}"
                if key in seen_keys:
                    continue

                notif = {
                    "id": "n_" + uuid.uuid4().hex[:10],
                    "source": source,
                    "watcher_id": entry.get("watcher_id", ""),
                    "external_id": str(entry.get("external_id", "")),
                    "title": entry.get("title", f"{source} update"),
                    "thumb": entry.get("thumb", ""),
                    "rating": entry.get("rating", ""),
                    "url": entry.get("url", ""),
                    "metadata": entry.get("metadata", {}),
                    "read": False,
                    "created_at": entry.get("created_at", now)
                }
                added.append(notif)
                seen_keys.add(key)

            if added:
                # Keep up to 2,000 most recent notifications
                updated = (added + existing_notifs)[:2000]
                raw["notifications"] = updated
                _atomic_save_json(self.notifs_file, raw)
            return added

    def list_notifications(self, limit: int = 100, unread_only: bool = False) -> List[Dict[str, Any]]:
        with _lock:
            raw = _load_json(self.notifs_file, {"notifications": []})
            items = raw.get("notifications", [])
            if unread_only:
                items = [n for n in items if not n.get("read", False)]
            return items[:limit]

    def mark_read(self, notif_id: Optional[str] = None) -> int:
        with _lock:
            raw = _load_json(self.notifs_file, {"notifications": []})
            updated_count = 0
            for n in raw.get("notifications", []):
                if notif_id is None or n.get("id") == notif_id:
                    if not n.get("read"):
                        n["read"] = True
                        updated_count += 1
            if updated_count:
                _atomic_save_json(self.notifs_file, raw)
            return updated_count

    def clear_notifications(self, source: Optional[str] = None) -> int:
        with _lock:
            raw = _load_json(self.notifs_file, {"notifications": []})
            items = raw.get("notifications", [])
            if source:
                keep = [n for n in items if n.get("source") != source]
                removed = len(items) - len(keep)
                raw["notifications"] = keep
            else:
                removed = len(items)
                raw["notifications"] = []
            if removed:
                _atomic_save_json(self.notifs_file, raw)
            return removed

    def get_summary(self) -> Dict[str, Any]:
        with _lock:
            raw = _load_json(self.notifs_file, {"notifications": []})
            items = raw.get("notifications", [])
            total = len(items)
            unread = sum(1 for n in items if not n.get("read"))
            by_source: Dict[str, int] = {}
            for n in items:
                if not n.get("read"):
                    src = n.get("source", "other")
                    by_source[src] = by_source.get(src, 0) + 1
            return {"total": total, "unread": unread, "by_source": by_source}
