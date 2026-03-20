# chess-data

Download and process CHESS data from ESS-DIVE, with variable mapping to [BERVO](https://github.com/bioepic-data/bervo).

## Setup

```bash
uv sync
export ESSDIVE_TOKEN=your_token_here
```

Get your ESS-DIVE token at: https://docs.ess-dive.lbl.gov/programmatic-tools/ess-dive-dataset-api#get-access

## Usage

```bash
# Search for CHESS datasets on ESS-DIVE
uv run chess search

# Download all CHESS datasets
uv run chess download --output-dir data/raw/

# Extract variables from downloaded data
uv run chess extract --input-dir data/raw/

# Map variables to BERVO terms
uv run chess map-bervo --input data/extracted/variables.tsv --bervo-terms bervo/bervo-terms.tsv
```

## Dependencies

- [bioepic_skills](https://github.com/bioepic-data/bioepic_skills) — ESS-DIVE search/extraction
- [BERVO](https://github.com/bioepic-data/bervo) — Variable ontology
