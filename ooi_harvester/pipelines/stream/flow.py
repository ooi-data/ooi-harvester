from prefect import task, Flow, Parameter
from prefect.utilities.logging import get_logger
from ooi_harvester.pipelines.stream.tasks import (
    get_stream_harvest,
    setup_harvest,
    request_data,
    check_data,
    get_response,
    setup_process,
    data_processing,
    finalize_data_stream,
    data_availability,
)


def create_flow(
    name="stream_harvest",
    storage=None,
    run_config=None,
    state_handlers=None,
    schedule=None,
    log_handler_settings=None,
    **kwargs
):
    with Flow(
        name=name,
        storage=storage,
        run_config=run_config,
        state_handlers=state_handlers,
        schedule=schedule,
        **kwargs
    ) as flow:
        config = Parameter("config", required=True)
        target_bucket = Parameter("target_bucket", default="s3://ooi-data")
        max_data_chunk = Parameter("max_chunk", default="100MB")
        export_da = Parameter("export_da", default=False)

        # Producer
        stream_harvest = get_stream_harvest(config)
        estimated_request = setup_harvest(stream_harvest)
        data_response = request_data(estimated_request, stream_harvest)

        # Data checking
        data_readiness = check_data(data_response)
        response_json = get_response(data_readiness)

        # Process data to temp
        nc_files_dict = setup_process(response_json, target_bucket)
        stores_dict = data_processing(
            nc_files_dict, stream_harvest, max_data_chunk
        )

        # Finalize data and transfer to final
        final_path = finalize_data_stream(
            stores_dict, stream_harvest, max_data_chunk
        )

        # Data availability
        availability = data_availability(
            nc_files_dict, stream_harvest, export_da
        )
        availability.set_upstream(final_path)

    # TODO: Use log handler settings to setup below
    # task_names = [t.name for t in flow.tasks]
    # fs_kwargs = dict(
    #     client_kwargs=dict(endpoint_url="http://localhost:9000/"),
    #     key="minioadmin",
    #     secret="minioadmin",
    # )
    # flow_logger = get_logger()
    # flow_logger.addHandler(HarvestFlowLogHandler(task_names, fs_kwargs=fs_kwargs))
    return flow
