import json
import os
import sys
import urllib.request

OWNER_REPO = os.environ.get("GITHUB_REPO", "TallanLem/ForGlory")
TAG = os.environ.get("DB_RELEASE_TAG", "db-latest")
ASSET_NAME = os.environ.get("DB_ASSET_NAME", "ratings.sqlite")
OUT_PATH = os.environ.get("DB_PATH", "data/db/ratings.sqlite")
TOKEN = os.environ.get("GITHUB_TOKEN", "")

api_url = f"https://api.github.com/repos/{OWNER_REPO}/releases/tags/{TAG}"

req = urllib.request.Request(api_url, headers={"Accept": "application/vnd.github+json"})
if TOKEN:
    req.add_header("Authorization", f"Bearer {TOKEN}")

with urllib.request.urlopen(req) as r:
    data = json.load(r)

download_url = None
for a in data.get("assets", []):
    if a.get("name") == ASSET_NAME:
        download_url = a.get("browser_download_url")
        break

if not download_url:
    print(f"ERROR: asset '{ASSET_NAME}' not found in release tag '{TAG}' for {OWNER_REPO}", file=sys.stderr)
    sys.exit(2)

os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)

print("Downloading:", download_url)
urllib.request.urlretrieve(download_url, OUT_PATH)
print("Saved to:", OUT_PATH)
