import typer
import textwrap

from .producer import (
    fetch_instrument_streams_list,
    create_request_estimate,
    perform_request,
    _sort_and_filter_estimated_requests,
)
from .utils.compute import map_concurrency

app = typer.Typer()


@app.command()
def producer(
    instrument_rd: str,
    refresh: bool = False,
    existing_data_path: str = "s3://ooi-data",
):
    streams_list = fetch_instrument_streams_list(instrument_rd)
    estimated_requests = map_concurrency(
        create_request_estimate,
        streams_list,
        func_kwargs=dict(
            refresh=refresh, existing_data_path=existing_data_path
        ),
        max_workers=50,
    )
    estimated_dict = _sort_and_filter_estimated_requests(estimated_requests)
    success_requests = estimated_dict['success_requests']
    request_responses = []
    if len(success_requests) > 0:
        request_responses = [
            perform_request(req, refresh) for req in success_requests
        ]

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
