import os
import json
import zipfile
import shutil

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DIST_DIR = os.path.join(ROOT_DIR, "dist")


def build():
    manifest_path = os.path.join(ROOT_DIR, "manifest.json")
    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    ext_id = manifest["id"]
    version = manifest["version"]
    zip_name = f"{ext_id}-{version}.zip"

    os.makedirs(DIST_DIR, exist_ok=True)
    zip_path = os.path.join(DIST_DIR, zip_name)

    exclude_dirs = {"__pycache__", ".git", ".github", ".venv", "dist", ".pytest_cache"}
    exclude_exts = {".pyc", ".pyo", ".pyd"}

    print(f"Creating release archive: {zip_path}")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(ROOT_DIR):
            dirs[:] = [d for d in dirs if d not in exclude_dirs]
            if root.startswith(DIST_DIR):
                continue

            for file in files:
                _, ext = os.path.splitext(file)
                if ext in exclude_exts or file.endswith(".zip"):
                    continue

                abs_file = os.path.join(root, file)
                rel_file = os.path.relpath(abs_file, ROOT_DIR)
                # Archive with top-level folder
                archive_name = os.path.join(f"{ext_id}-{version}", rel_file)
                zf.write(abs_file, archive_name)

    print(f"Release archive built successfully: {zip_path} ({os.path.getsize(zip_path)} bytes)")
    return zip_path


if __name__ == "__main__":
    build()
