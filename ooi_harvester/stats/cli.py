import datetime

import typer
from . import create_stats

app = typer.Typer()


@app.command()
def create(
    s3_bucket: str = "ooi-data",
):
    typer.echo("Data stats creation/refresh started.")
    start_time = datetime.datetime.utcnow()
    create_stats(s3_bucket)
    time_elapsed = datetime.datetime.utcnow() - start_time
    typer.echo(
        f"Data stats creation/refresh finished. Process took {str(time_elapsed)}"
    )


if __name__ == "__main__":
    app()
