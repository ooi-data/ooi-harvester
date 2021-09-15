import pytest
import requests
import fsspec

AWS_KEY = "minioadmin"
AWS_SECRET = "minioadmin"


@pytest.fixture(autouse=True)
def set_env(monkeypatch):
    monkeypatch.setenv("AWS_KEY", AWS_KEY)
    monkeypatch.setenv("AWS_SECRET", AWS_SECRET)
    monkeypatch.setenv("OOI_USERNAME", "username")
    monkeypatch.setenv("OOI_TOKEN", "token")


@pytest.fixture
def harvest_settings():
    from ooi_harvester.settings import harvest_settings  # noqa

    return harvest_settings


def is_responsive(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return True
    except ConnectionError:
        return False


@pytest.fixture(scope="session")
def minio_service(docker_ip, docker_services):
    """Ensure that Minio service is up and responsive."""

    # `port_for` takes a container port and returns the corresponding host port
    port = docker_services.port_for("minio", 9000)
    url = "http://{}:{}".format(docker_ip, port)

    common_storage_options = dict(
        client_kwargs=dict(endpoint_url=url),
        key=AWS_KEY,
        secret=AWS_SECRET,
    )
    bucket_name = "ooi-data"
    fs = fsspec.filesystem(
        "s3",
        **common_storage_options,
    )
    if not fs.exists(bucket_name):
        fs.mkdir(bucket_name)

    # Setup temp bucket
    temp_bucket = f"temp-{bucket_name}"
    if not fs.exists(temp_bucket):
        fs.mkdir(temp_bucket)
    return common_storage_options
