"""ESS-DIVE API client for CHESS data discovery and download."""
import json
import os
from pathlib import Path

import requests

ESSDIVE_API = "https://api.ess-dive.lbl.gov"


def get_token() -> str:
    """Get ESS-DIVE API token from environment."""
    token = os.environ.get("ESSDIVE_TOKEN")
    if not token:
        raise ValueError(
            "ESSDIVE_TOKEN not set. "
            "Get token at: https://docs.ess-dive.lbl.gov/programmatic-tools/ess-dive-dataset-api#get-access"
        )
    return token


def search_datasets(keyword: str = "CHESS", page_size: int = 20) -> list[dict]:
    """Search ESS-DIVE for datasets matching keyword."""
    token = get_token()
    headers = {"Authorization": f"bearer {token}"}
    params = {"query": keyword, "rows": page_size}
    resp = requests.get(f"{ESSDIVE_API}/packages", headers=headers, params=params)
    resp.raise_for_status()
    data = resp.json()
    results = []
    for pkg in data.get("results", []):
        ds = pkg.get("dataset", {})
        results.append({
            "id": pkg.get("id", ""),
            "title": ds.get("name", ""),
            "doi": ds.get("doi", ""),
            "description": ds.get("description", ""),
        })
    return results


def download_datasets(output_dir: str = "data/raw/", dataset_ids: list[str] | None = None):
    """Download CHESS datasets from ESS-DIVE."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    if dataset_ids is None:
        results = search_datasets("CHESS", page_size=100)
        dataset_ids = [r["id"] for r in results]
        print(f"Found {len(dataset_ids)} CHESS datasets")

    token = get_token()
    headers = {"Authorization": f"bearer {token}"}

    for did in dataset_ids:
        print(f"Downloading {did}...")
        resp = requests.get(f"{ESSDIVE_API}/packages/{did}", headers=headers)
        resp.raise_for_status()
        pkg = resp.json()

        pkg_dir = output_path / did
        pkg_dir.mkdir(exist_ok=True)
        with open(pkg_dir / "metadata.json", "w") as f:
            json.dump(pkg, f, indent=2)

        for file_info in pkg.get("dataset", {}).get("distribution", []):
            url = file_info.get("contentUrl")
            name = file_info.get("name", url.split("/")[-1] if url else "unknown")
            if url:
                print(f"  Fetching {name}...")
                file_resp = requests.get(url, headers=headers)
                file_resp.raise_for_status()
                with open(pkg_dir / name, "wb") as f:
                    f.write(file_resp.content)

    print(f"Downloaded {len(dataset_ids)} datasets to {output_dir}")
