from ooi_harvester.settings.main import harvest_settings

# OOI Config
OOI_USERNAME = harvest_settings.ooi_config.username
OOI_TOKEN = harvest_settings.ooi_config.token
BASE_URL = harvest_settings.ooi_config.base_urls.get('ooinet')
M2M_PATH = harvest_settings.ooi_config.paths.get('m2m')

# OOI Raw Config
RAW_BASE_URL = harvest_settings.ooi_config.base_urls.get('raw')
RAW_PATH = harvest_settings.ooi_config.paths.get('raw')

# Storage Options
STORAGE_OPTIONS = harvest_settings.storage_options.dict()

METADATA_BUCKET = harvest_settings.s3_buckets.get('metadata')
HARVEST_CACHE_BUCKET = harvest_settings.s3_buckets.get('harvest_cache')

# Github
GH_PAT = harvest_settings.github.pat
GH_DATA_ORG = harvest_settings.github.data_org
GH_MAIN_BRANCH = harvest_settings.github.main_branch
RESPONSE_PATH_STR = harvest_settings.github.defaults.response_path_str
REQUEST_STATUS_PATH_STR = (
    harvest_settings.github.defaults.request_status_path_str
)
PROCESS_STATUS_PATH_STR = (
    harvest_settings.github.defaults.process_status_path_str
)
CONFIG_PATH_STR = harvest_settings.github.defaults.config_path_str

COMMIT_MESSAGE_TEMPLATE = (
    harvest_settings.github.defaults.commit_message_template
)

PROCESS_COMMIT_MESSAGE_TEMPLATE = (
    harvest_settings.github.defaults.process_commit_message_template
)

STATUS_EMOJIS = harvest_settings.github.defaults.status_emojis
