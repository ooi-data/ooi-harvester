import datetime
import dateutil
import time

import requests
from loguru import logger

from ..utils.parser import parse_response_thredds, filter_and_parse_datasets


def data_status_check(response):
    in_progress = True
    name = response['stream']['table_name']
    if 'status_url' in response['result']:
        while in_progress:
            in_progress = check_in_progress(response['result']['status_url'])
            if not in_progress:
                return "success", f"{name} ::: Data available for download"
            time_since_request = (
                datetime.datetime.utcnow()
                - dateutil.parser.parse(response['result']['request_dt'])
            )
            if time_since_request > datetime.timedelta(days=2):
                catalog_dict = parse_response_thredds(response)
                filtered_catalog_dict = filter_and_parse_datasets(catalog_dict)
                if len(filtered_catalog_dict['datasets']) > 0:
                    return (
                        "success",
                        f"{name} ::: Data request timeout reached. But nc files are still available.",
                    )
                else:
                    return (
                        "fail",
                        f"{name} ::: Data request timeout reached. Has been waiting for more than 2 days. ({str(time_since_request)})",
                    )
            else:
                logger.info(
                    f"{name} ::: Data request time elapsed:"
                    f" {str(time_since_request)}"
                )
                time.sleep(10)
    else:
        return "skip", f"{name} ::: Skipping"


def check_in_progress(status_url):
    """Look for the existance of the status.txt"""
    r = requests.get(status_url)
    if r.status_code == 200:
        return False
    return True
