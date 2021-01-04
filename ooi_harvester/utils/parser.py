import datetime
import math

from loguru import logger

from ..config import STORAGE_OPTIONS


def estimate_size_and_time(raw):
    m = ""
    if "requestUUID" in raw:
        est_size = raw["sizeCalculation"] / 1024 ** 2
        size_txt = "MB"
        if (est_size / 1024) >= 1.0:
            est_size = est_size / 1024
            size_txt = "GB"

        est_time = raw["timeCalculation"]
        time_txt = "Seconds"
        if (est_time / 60) >= 1.0 and (est_time / 60) < 60.0:
            est_time = math.floor(est_time / 60)
            time_txt = "Minutes"
            if est_time == 1:
                time_txt = "Minute"
        elif (est_time / 60) >= 60.0:
            est_time = math.floor(est_time / 60 ** 2)
            time_txt = "Hours"
            if est_time == 1:
                time_txt = "Hour"
        m = f"""
        Estimated File size: {est_size:.4} {size_txt}
        Estimated Time: {est_time} {time_txt}
        """
    elif "message" in raw:
        m = f"""
        No estimate calculated.
        {raw['message']['status']}
        """
    return m


def parse_uframe_response(resp):
    if "allURLs" in resp:
        return {
            "request_id": resp["requestUUID"],
            "thredds_catalog": resp["allURLs"][0],
            "download_catalog": resp["allURLs"][1],
            "status_url": resp["allURLs"][1] + "/status.txt",
            "data_size": resp["sizeCalculation"],
            "estimated_time": resp["timeCalculation"],
            "units": {
                "data_size": "bytes",
                "estimated_time": "seconds",
                "request_dt": "UTC",
            },
            "request_dt": datetime.datetime.utcnow().isoformat(),
        }
    logger.warning(resp)
    return None


def seconds_to_date(num):
    start_dt = datetime.datetime(1900, 1, 1)
    return start_dt + datetime.timedelta(seconds=num)


def get_storage_options(path):
    if path.startswith("s3://"):
        return STORAGE_OPTIONS["aws"]
