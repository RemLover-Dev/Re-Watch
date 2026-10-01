import time
import threading
import logging
from typing import Optional, Callable, Dict, Any

from watcher_core.store import WatcherStore
from watcher_core.sources.gelbooru import check_gelbooru

logger = logging.getLogger("RemsWatcher.Scheduler")


class WatcherScheduler:
    """Centralized, thread-safe background scheduler for all active watchers."""

    def __init__(self, store: WatcherStore, on_update_callback: Optional[Callable] = None, proxy_url: str = None):
        self.store = store
        self.on_update = on_update_callback
        self.proxy_url = proxy_url
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._wakeup = threading.Event()
        self._manual_lock = threading.Lock()

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, name="RemsWatcherScheduler", daemon=True)
        self._thread.start()
        logger.info("Watcher scheduler daemon started.")

    def stop(self):
        self._running = False
        self._wakeup.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2.0)
        logger.info("Watcher scheduler daemon stopped.")

    def wakeup(self):
        """Signal the loop that watchers changed or immediate check requested."""
        self._wakeup.set()

    def _due_watchers(self):
        now = time.time()
        due = []
        for w in self.store.list_watchers():
            if not w.get("enabled", True):
                continue
            last = float(w.get("last_checked_at") or 0)
            interval_sec = float(w.get("interval_minutes") or 30) * 60
            if now >= (last + interval_sec):
                due.append(w)
        return due

    def _seconds_until_next_due(self) -> float:
        now = time.time()
        enabled = [w for w in self.store.list_watchers() if w.get("enabled", True)]
        if not enabled:
            return 60.0

        due_times = [
            float(w.get("last_checked_at") or 0) + float(w.get("interval_minutes") or 30) * 60
            for w in enabled
        ]
        return max(1.0, min(due_times) - now)

    def check_watcher(self, watcher: Dict[str, Any]) -> Dict[str, Any]:
        """Dispatch watcher to matching source checker."""
        source = str(watcher.get("source") or "").lower()
        if source == "gelbooru":
            return check_gelbooru(watcher, self.store, proxy_url=self.proxy_url)
        else:
            return {"new": 0, "error": f"Unsupported watcher source: {source}"}

    def check_now(self, source: Optional[str] = None) -> bool:
        """Trigger an immediate asynchronous check for all enabled watchers."""
        if not self._manual_lock.acquire(blocking=False):
            return False  # Run already in progress

        def _worker():
            try:
                watchers = self.store.list_watchers(source=source)
                total_new = 0
                for w in watchers:
                    if not w.get("enabled", True):
                        continue
                    res = self.check_watcher(w)
                    total_new += res.get("new", 0)

                if self.on_update:
                    try:
                        summary = self.store.get_summary()
                        self.on_update({"new": total_new, "summary": summary})
                    except Exception as e:
                        logger.error(f"Callback error after check_now: {e}")
            finally:
                self._manual_lock.release()

        threading.Thread(target=_worker, name="RemsWatcherCheckNow", daemon=True).start()
        return True

    def _run_loop(self):
        # Initial brief pause to allow main app startup
        time.sleep(3)
        while self._running:
            try:
                for w in self._due_watchers():
                    if not self._running:
                        break
                    res = self.check_watcher(w)
                    if res.get("new", 0) > 0 and self.on_update:
                        try:
                            summary = self.store.get_summary()
                            self.on_update({"new": res.get("new"), "summary": summary})
                        except Exception as e:
                            logger.error(f"Callback error in run_loop: {e}")

                wait_sec = min(300.0, self._seconds_until_next_due())
                self._wakeup.wait(timeout=wait_sec)
                self._wakeup.clear()
            except Exception as e:
                logger.exception(f"Unhandled error in scheduler loop: {e}")
                time.sleep(5)
