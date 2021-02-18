import os
import subprocess
from ..config import RESPONSE_PATH_STR, GH_DATA_ORG


def commit(
    message='',
    name="CAVA Bot",
    email="77078333+ooi-data-bot@users.noreply.github.com",
):
    subprocess.Popen(['git', 'config', '--global', 'user.email', email])
    subprocess.Popen(['git', 'config', '--global', 'user.name', name])
    subprocess.Popen(['git', 'add', '.'])
    subprocess.Popen(['git', 'commit', '-m', message])


def push():
    subprocess.Popen(['git', 'push'])


def get_status_json(table_name, request_dt, status):
    """
    Example yaml
    ------------
    data_stream: CE01ISSM-MFD35-02-PRESFA000-recovered_host-presf_abc_dcl_tide_measurement_recovered
    status: success
    data_ready: true
    last_request: 2021-01-07T23:30:55.159Z
    response: https://raw.githubusercontent.com/ooi-data/CE01ISSM-MFD35-02-PRESFA000-recovered_host-presf_abc_dcl_tide_measurement_recovered/main/history/response.json
    """
    status_json = {}
    status_json['data_stream'] = table_name
    status_json['status'] = status
    status_json['data_ready'] = False
    status_json['last_request'] = request_dt + 'Z'
    status_json[
        'response'
    ] = f"https://raw.githubusercontent.com/{GH_DATA_ORG}/{table_name}/main/{RESPONSE_PATH_STR}"  # noqa
    return status_json


def get_process_status_json(
    table_name, data_bucket, last_updated, status, data_start, data_end
):
    """
    Example yaml
    ------------
    data_stream: CE01ISSM-MFD35-02-PRESFA000-recovered_host-presf_abc_dcl_tide_measurement_recovered
    data_location: s3://ooi-data/CE01ISSM-MFD35-02-PRESFA000-recovered_host-presf_abc_dcl_tide_measurement_recovered
    status: success
    last_updated: 2021-01-07T23:30:55.159Z
    start_date: 2014-11-03T22:08:04.000Z
    end_date: 2020-08-18T01:23:18.820Z
    """
    status_json = {}
    status_json['data_stream'] = table_name
    status_json['data_location'] = os.path.join(data_bucket, table_name)
    status_json['status'] = status
    status_json['last_updated'] = last_updated + 'Z'
    status_json['start_date'] = data_start
    status_json['end_date'] = data_end
    return status_json
