import typer
import textwrap

from .producer import fetch_harvest
from .metadata import cli as metadata_cli

app = typer.Typer()
app.add_typer(metadata_cli.app, name="metadata")


@app.command()
def producer(
    instrument_rd: str,
    refresh: bool = False,
    existing_data_path: str = "s3://ooi-data",
):
    request_responses = fetch_harvest(
        instrument_rd, refresh, existing_data_path
    )

    if len(request_responses) == 0:
        typer.echo("WARNING: No requests to be fetched.")
    else:
        all_resp = []
        for r in request_responses:
            resp_text = textwrap.dedent(
                f"""\
                        {r['stream']['table_name']}
                        Refresh: {refresh}
                        Request Range: {r['params']['beginDT']} - {r['params']['endDT']}
                        Thredds: {r['result']['thredds_catalog']}
                        """
            )
            all_resp.append(resp_text)
        typer.echo('\n'.join(all_resp))


if __name__ == "__main__":
    app()
