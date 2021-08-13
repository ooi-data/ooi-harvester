from pydantic import BaseSettings
from ooi_harvester.settings.models import (
    StorageOptions,
    OOIConfig,
    S3Buckets,
    GithubConfig,
)


class HarvestSettings(BaseSettings):
    storage_options: StorageOptions = StorageOptions()
    s3_buckets: S3Buckets = S3Buckets()
    ooi_config: OOIConfig = OOIConfig()
    github: GithubConfig = GithubConfig()


harvest_settings = HarvestSettings()
