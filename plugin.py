import os
import sys
import logging
from flask import request, jsonify

# Add plugin folder to sys.path so internal imports resolve cleanly
_plugin_dir = os.path.dirname(os.path.abspath(__file__))
if _plugin_dir not in sys.path:
    sys.path.insert(0, _plugin_dir)

from watcher_core.store import WatcherStore
from watcher_core.scheduler import WatcherScheduler

logger = logging.getLogger("RemsWatcher.Plugin")


class RemsWatcherPlugin:
    def __init__(self, app, socketio, context):
        self.app = app
        self.socketio = socketio
        self.context = context
        self.data_dir = os.path.join(context.get("database_dir", ""), "watcher_data")
        self.store = WatcherStore(self.data_dir)
        self.scheduler = None

    def setup(self):
        logger.info("Initializing Rems Watcher Plugin...")
        proxy_url = os.getenv("PROXY_URL", "http://127.0.0.1:10808") if os.getenv("USE_PROXY", "").lower() == "true" else None

        def on_update(payload):
            if self.socketio:
                try:
                    self.socketio.emit("notifications_update", payload)
                except Exception as e:
                    logger.error(f"Failed to emit notifications_update: {e}")

        self.scheduler = WatcherScheduler(self.store, on_update_callback=on_update, proxy_url=proxy_url)
        self._register_routes()
        self.scheduler.start()
        logger.info("Rems Watcher Plugin setup complete.")

    def teardown(self):
        logger.info("Tearing down Rems Watcher Plugin...")
        if self.scheduler:
            self.scheduler.stop()
            self.scheduler = None

    def _register_routes(self):
        app = self.app
        if not app:
            return

        # Use unique endpoint names to avoid conflicts
        @app.route("/api/watchers", methods=["GET", "POST"], endpoint="ext_rems_watchers_list_create")
        def api_watchers():
            if request.method == "POST":
                data = request.json or {}
                source = data.get("source", "gelbooru")
                tags = data.get("tags", [])
                ratings = data.get("ratings", [])
                interval = data.get("interval_minutes", 30)

                w, err = self.store.create_watcher(source=source, tags=tags, ratings=ratings, interval_minutes=interval)
                if err:
                    return jsonify({"success": False, "error": err}), 400
                if self.scheduler:
                    self.scheduler.wakeup()
                return jsonify({"success": True, "watcher": w})

            return jsonify({"success": True, "watchers": self.store.list_watchers()})

        @app.route("/api/watchers/<wid>", methods=["DELETE"], endpoint="ext_rems_watchers_delete")
        def api_delete_watcher(wid):
            ok = self.store.remove_watcher(wid)
            if self.scheduler:
                self.scheduler.wakeup()
            return jsonify({"success": ok})

        @app.route("/api/watchers/check_now", methods=["POST"], endpoint="ext_rems_watchers_check_now")
        def api_check_now():
            data = request.json or {}
            source = data.get("source")
            if self.scheduler:
                started = self.scheduler.check_now(source=source)
                return jsonify({"success": True, "started": started})
            return jsonify({"success": False, "error": "Scheduler not running"}), 503

        @app.route("/api/notifications", methods=["GET", "DELETE"], endpoint="ext_rems_notifs_list_clear")
        def api_notifications():
            if request.method == "DELETE":
                cleared = self.store.clear_notifications()
                return jsonify({"success": True, "cleared": cleared})

            limit = int(request.args.get("limit", 100))
            unread_only = request.args.get("unread_only", "").lower() == "true"
            items = self.store.list_notifications(limit=limit, unread_only=unread_only)
            summary = self.store.get_summary()
            return jsonify({"success": True, "notifications": items, "summary": summary})

        @app.route("/api/notifications/mark_read", methods=["POST"], endpoint="ext_rems_notifs_mark_read")
        def api_mark_read():
            data = request.json or {}
            nid = data.get("id")
            updated = self.store.mark_read(notif_id=nid)
            return jsonify({"success": True, "updated": updated, "summary": self.store.get_summary()})


def setup(app, socketio, context):
    """Extension Manager standard entry point function."""
    plugin = RemsWatcherPlugin(app, socketio, context)
    plugin.setup()
    return plugin
