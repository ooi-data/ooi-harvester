from pydantic import BaseSettings, BaseModel, Field, PyObject, validator
import prefect


def get_prefect_secret(key):
    prefect_secret = prefect.client.Secret(key)
    if prefect_secret.exists():
        return prefect_secret.get()
    return None


class GithubStatusDefaults(BaseModel):
    status_emojis: dict = {
        "pending": "🔵",
        "failed": "🔴",
        "success": "🟢",
        "skip": "🟠",
        "discontinued": "⚫️",
    }
    process_commit_message_template: PyObject = (
        "{status_emoji} Data processing [{status}] ({request_dt})".format
    )
    commit_message_template: PyObject = (
        "{status_emoji} Data request [{status}] ({request_dt})".format
    )
    config_path_str: str = 'config.yaml'
    process_status_path_str: str = 'history/process.yaml'
    request_status_path_str: str = 'history/request.yaml'
    response_path_str: str = 'history/response.json'


class GithubConfig(BaseSettings):
    defaults: GithubStatusDefaults = GithubStatusDefaults()
    main_branch: str = Field('main')
    data_org: str = Field('ooi-data')
    pat: str = Field(None, env="gh_pat")

    @validator('pat')
    def pat_prefect(cls, v):
        if v is None:
            return get_prefect_secret("GH_PAT")
        return v


class AWSConfig(BaseSettings):
    key: str = Field(None, env="aws_key")
    secret: str = Field(None, env="aws_secret")

    @validator('key')
    def key_prefect(cls, v):
        if v is None:
            return get_prefect_secret("AWS_KEY")
        return v

    @validator('secret')
    def secret_prefect(cls, v):
        if v is None:
            return get_prefect_secret("AWS_KEY")
        return v


class S3Buckets(BaseModel):
    metadata: str = 'ooi-metadata'
    harvest_cache: str = 'io2data-harvest-cache'

    @validator('*', pre=True, always=True)
    def add_s3(cls, v):
        if "s3://" not in v:
            return f"s3://{v}"
        return v


class OOIConfig(BaseSettings):
    username: str = Field(None, env="ooi_username")
    token: str = Field(None, env="ooi_token")
    base_urls: dict = {
        "ooinet": "https://ooinet.oceanobservatories.org",
        "rawdata": "https://rawdata.oceanobservatories.org",
        "cava": "https://api.interactiveoceans.washington.edu",
    }
    paths: dict = {"m2m": "api/m2m", "raw": "files"}
    time: dict = {
        'units': 'seconds since 1900-01-01 0:0:0',
        'calendar': 'gregorian',
    }

    @validator('username')
    def username_prefect(cls, v):
        if v is None:
            return get_prefect_secret("OOI_USERNAME")
        return v

    @validator('token')
    def token_prefect(cls, v):
        if v is None:
            return get_prefect_secret("OOI_TOKEN")
        return v


class StorageOptions(BaseSettings):
    aws: AWSConfig = AWSConfig()
