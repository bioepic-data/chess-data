"""Extract variables from downloaded CHESS datasets."""
import csv
import json
from pathlib import Path


def extract_variables(input_dir: str = "data/raw/", output_path: str = "data/extracted/variables.tsv"):
    """Extract variable names and metadata from downloaded CHESS datasets."""
    input_path = Path(input_dir)
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    variables = []

    for pkg_dir in sorted(input_path.iterdir()):
        if not pkg_dir.is_dir():
            continue

        meta_file = pkg_dir / "metadata.json"
        if not meta_file.exists():
            continue

        with open(meta_file) as f:
            meta = json.load(f)

        dataset_name = meta.get("dataset", {}).get("name", pkg_dir.name)
        dataset_id = pkg_dir.name

        # Extract from NetCDF files
        for nc_file in pkg_dir.glob("*.nc"):
            try:
                import xarray as xr

                ds = xr.open_dataset(nc_file)
                for var_name, var in ds.data_vars.items():
                    variables.append({
                        "dataset_id": dataset_id,
                        "dataset_name": dataset_name,
                        "source_file": nc_file.name,
                        "variable_name": var_name,
                        "long_name": var.attrs.get("long_name", ""),
                        "units": var.attrs.get("units", ""),
                        "dimensions": str(var.dims),
                    })
                ds.close()
            except Exception as e:
                print(f"  Warning: Could not read {nc_file}: {e}")

        # Extract from CSV files
        for csv_file in pkg_dir.glob("*.csv"):
            try:
                with open(csv_file, newline="") as f:
                    reader = csv.reader(f)
                    headers = next(reader, None)
                    if headers:
                        for h in headers:
                            variables.append({
                                "dataset_id": dataset_id,
                                "dataset_name": dataset_name,
                                "source_file": csv_file.name,
                                "variable_name": h.strip(),
                                "long_name": "",
                                "units": "",
                                "dimensions": "",
                            })
            except Exception as e:
                print(f"  Warning: Could not read {csv_file}: {e}")

    if variables:
        fieldnames = ["dataset_id", "dataset_name", "source_file", "variable_name", "long_name", "units", "dimensions"]
        with open(output_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
            writer.writeheader()
            writer.writerows(variables)
        print(f"Extracted {len(variables)} variables to {output_path}")
    else:
        print("No variables found")
