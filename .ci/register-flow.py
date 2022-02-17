import datetime
import os
from pathlib import Path
from prefect.storage.docker import Docker
from ooi_harvester.pipelines.stream.flow import create_flow

HERE = Path(__file__).resolve().parent

now = datetime.datetime.utcnow()
image_registry = "cormorack"
image_name = "ooi-harvester"
image_tag = f"{now:%Y%m%dT%H%M}"

docker_storage = Docker(
    registry_url=image_registry,
    dockerfile=HERE.joinpath("Dockerfile"),
    image_name=image_name,
    prefect_directory="/home/jovyan/prefect",
    env_vars={'HARVEST_ENV': 'ooi-harvester'},
    python_dependencies=[
        'git+https://github.com/ooi-data/ooi-harvester.git@main'
    ],
    image_tag=image_tag,
    build_kwargs={
        'nocache': True,
        'buildargs': {
            'PYTHON_VERSION': os.environ.get('PYTHON_VERSION', '3.8')
        },
    },
)

flow = create_flow(storage=docker_storage)
