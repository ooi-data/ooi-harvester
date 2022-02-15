from prefect import Flow, Parameter
from prefect.utilities.logging import get_logger
from pydantic import BaseModel
from typing import Dict, Any, Union
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
from ooi_harvester.pipelines.stream.handlers import (
    HarvestFlowLogHandler,
    # get_main_flow_state_handler,
)
from ooi_harvester.pipelines.notifications.notifications import (
    github_issue_notifier,
)


class LogHandlerSettings(BaseModel):
    fs_protocol: str = "s3"
    fs_kwargs: Dict[str, Any] = {}
    bucket_name: str = "io2data-harvest-cache"


def create_flow(
    name="stream_harvest",
    storage=None,
    run_config=None,
    state_handlers=None,
    schedule=None,
    issue_config: Dict[str, Any] = {},
    log_settings: Union[LogHandlerSettings, Dict[str, Any]] = {},
    **kwargs
) -> Flow:

    # if state_handlers is None:
    #     main_flow_sh = get_main_flow_state_handler()
    #     state_handlers = [main_flow_sh]

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
            nc_files_dict,
            stream_harvest,
            max_data_chunk,
            state_handlers=[
                github_issue_notifier(
                    gh_org=issue_config.get("gh_org", "ooi-data"),
                    assignees=issue_config.get("assignees", []),
                    labels=issue_config.get("labels", []),
                )
            ],
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

    task_names = [t.name for t in flow.tasks]
    if isinstance(log_settings, dict):
        log_settings = LogHandlerSettings(**log_settings)
    elif isinstance(log_settings, LogHandlerSettings):
        ...
    else:
        raise TypeError("log_settings must be type LogHandlerSettings or Dict")

    flow_logger = get_logger()
    flow_logger.addHandler(
        HarvestFlowLogHandler(task_names, **log_settings.dict())
    )
    return flow
