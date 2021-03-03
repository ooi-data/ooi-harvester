import datetime

import typer
from . import create_metadata

app = typer.Typer()


@app.command()
def create(
    s3_bucket: str = "ooi-metadata",
    axiom: bool = False,
    global_ranges: bool = False,
    cava_assets: bool = False,
    ooinet_inventory: bool = False,
    ooi_streams: bool = False,
    instrument_catalog: bool = False,
):
    typer.echo("Metadata creation/refresh started.")
    start_time = datetime.datetime.utcnow()
    create_metadata(
        s3_bucket,
        axiom_refresh=axiom,
        global_ranges_refresh=global_ranges,
        cava_assets_refresh=cava_assets,
        ooinet_inventory_refresh=ooinet_inventory,
        ooi_streams_refresh=ooi_streams,
        instrument_catalog_refresh=instrument_catalog,
    )
    time_elapsed = datetime.datetime.utcnow() - start_time
    typer.echo(
        f"Metadata creation/refresh finished. Process took {str(time_elapsed)}"
    )


if __name__ == "__main__":
    app()
