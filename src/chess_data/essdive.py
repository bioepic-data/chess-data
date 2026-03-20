"""ESS-DIVE API client for CHESS data discovery and download.

Uses the public DataONE-compatible SOLR endpoint for search (no auth needed),
and the DataONE Member Node API for metadata retrieval.
For file downloads, ESSDIVE_TOKEN is needed.
"""
import json
import os
from pathlib import Path

import requests

# Public SOLR search endpoint (no auth needed)
SOLR_URL = "https://data.ess-dive.lbl.gov/catalog/d1/mn/v2/query/solr/"

# DataONE Member Node API (for object/metadata retrieval)
D1_URL = "https://data.ess-dive.lbl.gov/catalog/d1/mn/v2"

# ESS-DIVE authenticated API (for file downloads)
ESSDIVE_API = "https://api.ess-dive.lbl.gov"


def get_token() -> str | None:
    """Get ESS-DIVE API token from environment (optional for search)."""
    return os.environ.get("ESSDIVE_TOKEN")


def search_datasets(keyword: str = "CHESS", page_size: int = 50) -> list[dict]:
    """Search ESS-DIVE for datasets matching keyword via public SOLR endpoint.

    No authentication required.
    """
    params = {
        "q": keyword,
        "rows": page_size,
        "fl": "id,title,abstract,dateUploaded,author,keywords",
        "wt": "json",
    }
    resp = requests.get(SOLR_URL, params=params)
    resp.raise_for_status()
    data = resp.json()

    results = []
    for doc in data.get("response", {}).get("docs", []):
        results.append({
            "id": doc.get("id", ""),
            "title": doc.get("title", ""),
            "abstract": doc.get("abstract", ""),
            "date": doc.get("dateUploaded", ""),
            "author": doc.get("author", []),
            "keywords": doc.get("keywords", []),
        })

    total = data.get("response", {}).get("numFound", 0)
    print(f"Found {total} datasets matching '{keyword}'")
    return results


def get_dataset_metadata(dataset_id: str) -> dict:
    """Get full metadata for a dataset via DataONE API."""
    url = f"{D1_URL}/object/{dataset_id}"
    headers = {"Accept": "application/json"}
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    return resp.json()


def list_dataset_files(dataset_id: str) -> list[dict]:
    """List files in a dataset package via SOLR."""
    params = {
        "q": f'resourceMap:"{dataset_id}"',
        "rows": 500,
        "fl": "id,fileName,title,formatId,size,dateUploaded",
        "wt": "json",
    }
    resp = requests.get(SOLR_URL, params=params)
    resp.raise_for_status()
    data = resp.json()
    return data.get("response", {}).get("docs", [])


def download_file(file_id: str, output_path: Path) -> None:
    """Download a single file from ESS-DIVE via DataONE API."""
    url = f"{D1_URL}/object/{file_id}"
    token = get_token()
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    resp = requests.get(url, headers=headers, stream=True)
    resp.raise_for_status()

    with open(output_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)


def download_datasets(output_dir: str = "data/raw/", dataset_ids: list[str] | None = None):
    """Download CHESS datasets from ESS-DIVE.

    If dataset_ids is None, searches for all CHESS datasets first.
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    if dataset_ids is None:
        results = search_datasets("CHESS", page_size=100)
        dataset_ids = [r["id"] for r in results]

    for did in dataset_ids:
        print(f"\n=== Downloading {did} ===")
        pkg_dir = output_path / did.replace("/", "_")
        pkg_dir.mkdir(exist_ok=True)

        # Get file list
        files = list_dataset_files(did)
        print(f"  Found {len(files)} files")

        # Save file list as metadata
        with open(pkg_dir / "files.json", "w") as f:
            json.dump(files, f, indent=2)

        # Download each file
        for file_info in files:
            fid = file_info.get("id", "")
            fname = file_info.get("fileName", fid.split("/")[-1] if "/" in fid else fid)
            fmt = file_info.get("formatId", "")
            size = file_info.get("size", 0)

            # Skip resource maps and system metadata
            if "ore/terms" in fmt or "dataoneTypes" in fmt:
                continue

            size_mb = size / (1024 * 1024) if size else 0
            print(f"  Fetching {fname} ({size_mb:.1f} MB)...")

            try:
                download_file(fid, pkg_dir / fname)
            except Exception as e:
                print(f"  Warning: Failed to download {fname}: {e}")

    print(f"\nDownloaded {len(dataset_ids)} datasets to {output_dir}")
