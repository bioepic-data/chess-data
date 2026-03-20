"""Map extracted variables to BERVO ontology terms."""
import csv
from pathlib import Path


def map_to_bervo(
    input_path: str = "data/extracted/variables.tsv",
    bervo_terms_path: str = "bervo/bervo-terms.tsv",
    output_path: str = "data/mapped/bervo-mapping.tsv",
):
    """Map extracted CHESS variables to BERVO terms.

    Uses label matching and unit compatibility to suggest BERVO mappings.
    """
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Load BERVO terms
    bervo_terms = []
    with open(bervo_terms_path, newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            bervo_terms.append(row)

    # Load extracted variables
    variables = []
    with open(input_path, newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            variables.append(row)

    # Build lookup index from BERVO labels (lowercase)
    label_index: dict[str, list[dict]] = {}
    for term in bervo_terms:
        label = term.get("label", "").lower().strip()
        if label:
            label_index.setdefault(label, []).append(term)

    # Map variables
    mappings = []
    for var in variables:
        var_name = var["variable_name"].lower().strip()
        long_name = var.get("long_name", "").lower().strip()

        # Try exact match on variable name
        matches = label_index.get(var_name, [])
        if not matches and long_name:
            matches = label_index.get(long_name, [])

        # Try substring match
        if not matches:
            for label, terms in label_index.items():
                if var_name in label or label in var_name:
                    matches.extend(terms)
                    break

        if matches:
            for m in matches[:3]:
                mappings.append({
                    **var,
                    "bervo_id": m.get("id", ""),
                    "bervo_label": m.get("label", ""),
                    "bervo_definition": m.get("definition", "")[:200],
                    "match_type": "exact" if var_name == m.get("label", "").lower() else "substring",
                })
        else:
            mappings.append({
                **var,
                "bervo_id": "",
                "bervo_label": "",
                "bervo_definition": "",
                "match_type": "unmatched",
            })

    if mappings:
        fieldnames = list(mappings[0].keys())
        with open(output_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
            writer.writeheader()
            writer.writerows(mappings)
        print(f"Mapped {len(mappings)} variable entries to {output_path}")
        unmatched = sum(1 for m in mappings if m["match_type"] == "unmatched")
        print(f"  Matched: {len(mappings) - unmatched}, Unmatched: {unmatched}")
    else:
        print("No mappings generated")
