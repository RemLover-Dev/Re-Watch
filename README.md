# Rems Watcher 🔔

Modular 24/7 background tag monitoring, Pixiv follow tracking, unread counters, and instant notifications for **Rems Dl**.

## Features

- **Automated Tag Watchers**: Periodically poll Gelbooru (and extensible to any booru) for newly published artworks matching your favorite tags and rating filters.
- **Baseline Checkpoints**: Intelligent checkpoints ensure only new works uploaded after watcher creation trigger alerts—no historical spam.
- **Rate-Limit & Anti-Ban Protection**: Bounded scans, anti-ban pacing, and automatic pause to keep IP reputations safe.
- **Seamless Rems Dl Integration**: Installable as a 1-click modular extension inside Rems Dl's Settings tab, automatically downloaded from GitHub.
- **Standalone Daemon Mode**: Can also be executed headlessly on a home server, VPS, or background container.

## Installation via Rems Dl

1. Open **Rems Dl**.
2. Go to **Settings** (`Options` tab).
3. Scroll to **🧩 Extensions & Addons**.
4. Click **⬇️ Download & Install** next to **Rems Watcher**.
5. The **Notifications** tab will dynamically appear in your navigation menu!

## Standalone Usage

```bash
pip install -r requirements.txt
python -m watcher_core.scheduler
```

## License

MIT License.
