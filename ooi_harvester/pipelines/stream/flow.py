from prefect import Flow, Parameter
from prefect.utilities.logging import get_logger
from prefect.core.parameter import no_default
from pydantic import BaseModel
from typing import Dict, Any, Union, Optional
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
from ooi_harvester.settings.main import harvest_settings


class LogHandlerSettings(BaseModel):
    fs_protocol: str = "s3"
    fs_kwargs: Dict[str, Any] = {}
    bucket_name: str = "io2data-harvest-cache"


class FlowParameters(BaseModel):
    config: Optional[Dict[str, Any]]
    target_bucket: str = "s3://ooi-data"
    max_chunk: str = "100MB"
    export_da: bool = False
    gh_write_da: bool = False
    error_test: bool = False


def create_flow(
    name="stream_harvest",
    storage=None,
    run_config=None,
    default_params: Union[FlowParameters, Dict[str, Any]] = {},
    # state_handlers=None,
    schedule=None,
    issue_config: Dict[str, Any] = {},
    log_settings: Union[LogHandlerSettings, Dict[str, Any]] = {},
    **kwargs
) -> Flow:

    default_gh_org = harvest_settings.github.data_org

    issue_config.setdefault("gh_org", default_gh_org)

    state_handlers = [github_issue_notifier(**issue_config)]
    # if state_handlers is None:
    #     main_flow_sh = get_main_flow_state_handler()
    #     state_handlers = [main_flow_sh]

    # Check default_params
    if isinstance(default_params, dict):
        default_params = FlowParameters(**default_params)

    # Sets the defaults for flow config
    config_required = False
    if default_params.config is None:
        config_required = True

    default_dict = default_params.dict()

    with Flow(
        name=name,
        storage=storage,
        run_config=run_config,
        state_handlers=state_handlers,
        schedule=schedule,
        **kwargs
    ) as flow:
        config = Parameter(
            "config",
            required=config_required,
            default=default_dict.get("config", no_default),
        )
        target_bucket = Parameter(
            "target_bucket", default=default_dict.get("target_bucket")
        )
        max_data_chunk = Parameter(
            "max_chunk", default=default_dict.get("max_chunk")
        )
        export_da = Parameter(
            "export_da", default=default_dict.get("export_da")
        )
        gh_write_da = Parameter(
            "gh_write_da", default=default_dict.get("gh_write_da")
        )
        error_test = Parameter(
            "error_test", default=default_dict.get("error_test")
        )

        # Producer
        stream_harvest = get_stream_harvest(config)
        estimated_request = setup_harvest(
            stream_harvest,
            task_args={
                "state_handlers": state_handlers,
            },
        )
        data_response = request_data(
            estimated_request,
            stream_harvest,
            task_args={
                "state_handlers": state_handlers,
            },
        )

        # Data checking
        data_readiness = check_data(
            data_response,
            task_args={
                "state_handlers": state_handlers,
            },
        )
        response_json = get_response(data_readiness)

        # Process data to temp
        nc_files_dict = setup_process(response_json, target_bucket)
        stores_dict = data_processing(
            nc_files_dict,
            stream_harvest,
            max_data_chunk,
            error_test,
            task_args={
                "state_handlers": state_handlers,
            },
        )

        # Finalize data and transfer to final
        final_path = finalize_data_stream(
            stores_dict,
            stream_harvest,
            max_data_chunk,
            task_args={
                "state_handlers": state_handlers,
            },
        )

        # Data availability
        availability = data_availability(
            nc_files_dict,
            stream_harvest,
            export_da,
            gh_write_da,
            task_args={
                "state_handlers": state_handlers,
            },
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
