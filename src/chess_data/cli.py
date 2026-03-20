"""CLI for CHESS data operations."""
import click


@click.group()
def main():
    """CHESS data download and BERVO mapping tools."""
    pass


@main.command()
@click.option("--keyword", default="CHESS", help="Search keyword")
@click.option("--page-size", default=20, help="Results per page")
def search(keyword: str, page_size: int):
    """Search ESS-DIVE for CHESS datasets."""
    from chess_data.essdive import search_datasets

    results = search_datasets(keyword=keyword, page_size=page_size)
    for r in results:
        click.echo(f"{r['id']}: {r['title']}")


@main.command()
@click.option("--output-dir", default="data/raw/", help="Output directory")
@click.option("--dataset-ids", default=None, help="Comma-separated dataset IDs")
def download(output_dir: str, dataset_ids: str | None):
    """Download CHESS datasets from ESS-DIVE."""
    from chess_data.essdive import download_datasets

    ids = dataset_ids.split(",") if dataset_ids else None
    download_datasets(output_dir=output_dir, dataset_ids=ids)


@main.command()
@click.option("--input-dir", default="data/raw/", help="Input directory")
@click.option("--output", default="data/extracted/variables.tsv", help="Output file")
def extract(input_dir: str, output: str):
    """Extract variables from downloaded CHESS data."""
    from chess_data.extraction import extract_variables

    extract_variables(input_dir=input_dir, output_path=output)


@main.command("map-bervo")
@click.option("--input", "input_path", default="data/extracted/variables.tsv")
@click.option("--bervo-terms", default="bervo/bervo-terms.tsv")
@click.option("--output", default="data/mapped/bervo-mapping.tsv")
def map_bervo(input_path: str, bervo_terms: str, output: str):
    """Map extracted variables to BERVO ontology terms."""
    from chess_data.bervo_mapper import map_to_bervo

    map_to_bervo(input_path=input_path, bervo_terms_path=bervo_terms, output_path=output)


if __name__ == "__main__":
    main()
