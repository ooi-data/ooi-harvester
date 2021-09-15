from pathlib import Path
import yaml
import datetime
import json


def test_goldcopy_produce(harvest_settings, mocker):
    from ooi_harvester.producer import StreamHarvest
    from ooi_harvester.producer import (
        create_catalog_request,
    )
    from ooi_harvester.utils.github import get_status_json

    mocked_fetch_streams_list = mocker.patch(
        'ooi_harvester.producer.fetch_streams_list',
        spec=True,
        return_value=[
            {
                'parameter_ids': '7,10,11,12,16,333,353,354,355,356,358,863,932,933,935,936,937,938,939,2636,2708',
                'table_name': 'RS03AXPS-PC03A-4B-PHSENA302-streamed-phsen_data_record',
                'reference_designator': 'RS03AXPS-PC03A-4B-PHSENA302',
                'platform_code': 'RS03AXPS',
                'mooring_code': 'PC03A',
                'instrument_code': '4B-PHSENA302',
                'stream': 'phsen_data_record',
                'method': 'streamed',
                'count': 47940,
                'beginTime': '2014-10-02T23:00:00.284Z',
                'endTime': '2021-09-14T18:00:00.655Z',
                'stream_id': 112,
                'stream_rd': 'phsen_data_record',
                'stream_type': 'Science',
                'stream_content': 'Data Products',
                'last_updated': '2021-09-14T18:25:16.069803',
                'group_code': 'PHSEN',
            }
        ],
    )

    HERE = Path(__file__).parent.parent.absolute()
    BASE = HERE.joinpath('data')
    CONFIG_PATH = BASE.joinpath(
        harvest_settings.github.defaults.config_path_str
    )
    RESPONSE_PATH = BASE.joinpath(
        harvest_settings.github.defaults.response_path_str
    )
    REQUEST_STATUS_PATH = BASE.joinpath(
        harvest_settings.github.defaults.request_status_path_str
    )

    config_json = yaml.load(CONFIG_PATH.open(), Loader=yaml.SafeLoader)
    stream_harvest = StreamHarvest(**config_json)

    assert isinstance(config_json, dict)
    assert isinstance(stream_harvest, StreamHarvest)
    table_name = stream_harvest.table_name

    request_dt = datetime.datetime.utcnow().isoformat()
    if stream_harvest.harvest_options.goldcopy:
        streams_list = mocked_fetch_streams_list(stream_harvest)
        stream_dct = next(
            filter(lambda s: s['table_name'] == table_name, streams_list)
        )
        try:
            request_response = create_catalog_request(
                stream_dct,
                refresh=stream_harvest.harvest_options.refresh,
                existing_data_path=stream_harvest.harvest_options.path,
            )
            status_json = get_status_json(table_name, request_dt, 'pending')
        except Exception as e:
            status_json = get_status_json(table_name, request_dt, 'failed')
            request_response = {
                "stream": stream_dct,
                "result": {"message": str(e), "status": "failed"},
                "stream_name": table_name,
            }
    assert (
        request_response['stream_name']
        == 'RS03AXPS-PC03A-4B-PHSENA302-streamed-phsen_data_record'
    )
    assert request_response['stream'] == stream_dct

    RESPONSE_PATH.write_text(json.dumps(request_response))
    REQUEST_STATUS_PATH.write_text(yaml.dump(status_json))

    assert RESPONSE_PATH.exists()
    assert REQUEST_STATUS_PATH.exists()
