import os

# OOI Config
OOI_USERNAME = os.environ.get("OOI_USERNAME", None)
OOI_TOKEN = os.environ.get("OOI_TOKEN", None)
BASE_URL = "https://ooinet.oceanobservatories.org"
M2M_PATH = "api/m2m"

# OOI Raw Config
RAW_BASE_URL = "https://rawdata.oceanobservatories.org"
RAW_PATH = "files"

# Storage Options
STORAGE_OPTIONS = {
    "aws": {
        "key": os.environ.get("AWS_KEY", None),
        "secret": os.environ.get("AWS_SECRET", None),
    }
}

METADATA_BUCKET = 's3://ooi-metadata'
HARVEST_CACHE_BUCKET = 's3://io2data-harvest-cache'

# Github
GH_PAT = os.environ.get('GH_PAT', None)
RESPONSE_PATH_STR = 'history/response.json'
REQUEST_STATUS_PATH_STR = 'history/request.yaml'
PROCESS_STATUS_PATH_STR = 'history/process.yaml'
CONFIG_PATH_STR = 'config.yaml'

COMMIT_MESSAGE_TEMPLATE = (
    "{status_emoji} Data request [{status}] ({request_dt})".format
)

PROCESS_COMMIT_MESSAGE_TEMPLATE = (
    "{status_emoji} Data processing [{status}] ({request_dt})".format
)

STATUS_EMOJIS = {"pending": "🔵", "failed": "🔴", "success": "🟢", "skip": "⚫️"}
