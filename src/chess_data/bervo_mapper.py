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

    # Detect column names (BERVO TSV uses various formats)
    id_col = next((c for c in bervo_terms[0] if c.upper() == "ID"), "id")
    label_col = next((c for c in bervo_terms[0] if "label" in c.lower()), "label")
    def_col = next((c for c in bervo_terms[0] if "definition" in c.lower()), "definition")
    # Also check for EcoSIM variable name and synonyms
    ecosim_col = next((c for c in bervo_terms[0] if "ecosim variable" in c.lower()), None)
    synonym_col = next((c for c in bervo_terms[0] if "synonym" in c.lower()), None)

    # Build lookup indices (lowercase)
    label_index: dict[str, list[dict]] = {}
    for term in bervo_terms:
        # Skip header rows
        if term.get(id_col, "").startswith("ID"):
            continue
        # Index by label
        label = term.get(label_col, "").lower().strip()
        if label:
            label_index.setdefault(label, []).append(term)
        # Index by EcoSIM variable name
        if ecosim_col:
            ecosim_name = term.get(ecosim_col, "").lower().strip()
            if ecosim_name:
                label_index.setdefault(ecosim_name, []).append(term)
        # Index by synonyms
        if synonym_col:
            for syn in term.get(synonym_col, "").split("|"):
                syn = syn.lower().strip()
                if syn:
                    label_index.setdefault(syn, []).append(term)

    # Normalize variable names for matching
    def normalize(s: str) -> str:
        return s.lower().strip().replace("_", " ").replace("-", " ")

    # Map variables
    mappings = []
    for var in variables:
        var_name = normalize(var["variable_name"])
        long_name = normalize(var.get("long_name", ""))

        # Try exact match
        matches = label_index.get(var_name, [])
        if not matches and long_name:
            matches = label_index.get(long_name, [])

        # Try normalized match against index
        if not matches:
            for label, terms in label_index.items():
                norm_label = normalize(label)
                if var_name == norm_label or (long_name and long_name == norm_label):
                    matches.extend(terms)
                    break

        # Try substring match
        if not matches:
            for label, terms in label_index.items():
                norm_label = normalize(label)
                if len(var_name) > 3 and (var_name in norm_label or norm_label in var_name):
                    matches.extend(terms)
                    if len(matches) >= 3:
                        break

        if matches:
            seen = set()
            for m in matches:
                mid = m.get(id_col, "")
                if mid in seen:
                    continue
                seen.add(mid)
                if len(seen) > 3:
                    break
                mappings.append({
                    **var,
                    "bervo_id": mid,
                    "bervo_label": m.get(label_col, ""),
                    "bervo_definition": m.get(def_col, "")[:200],
                    "match_type": "exact" if var_name == normalize(m.get(label_col, "")) else "substring",
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
