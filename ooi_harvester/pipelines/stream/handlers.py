import logging
from logging import LogRecord
import datetime
from typing import Any, Dict, List, Union
import fsspec
import prefect
from prefect import Flow, Task  # noqa
from prefect.utilities.notifications import callback_factory
from ooi_harvester.processor.state_handlers import get_issue
from ooi_harvester.utils.parser import parse_exception

TrackedObjectType = Union["Flow", "Task"]


class HarvestFlowLogHandler(logging.StreamHandler):
    """
    Flow log handler for data harvest prefect pipeline

    Attributes
    ----------
    task_names : list
        The task names to include in log file.
    fs_protocol : str
        Protocol for the log storage,
        must be a valid system from fsspec.
    fs_kwargs : dict
        Kwargs for fsspec.filesystem.
    bucket_name : str
        The name of the bucket where
        logs should be stored.
    """

    def __init__(
        self,
        task_names: List[str],
        fs_protocol: str = "s3",
        fs_kwargs: Dict[str, Any] = {},
        bucket_name: str = "io2data-harvest-cache",
    ):
        super().__init__()
        self.task_names = task_names
        self.fs_kwargs = fs_kwargs
        self.fs_protocol = fs_protocol
        self.bucket_name = bucket_name
        self.__log_formatter = logging.Formatter(
            fmt="[%(asctime)s] %(levelname)s - %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S%z",
        )

    def emit(self, record: LogRecord) -> None:
        if hasattr(record, 'task_name'):
            task_name = getattr(record, 'task_name')
            if (
                task_name in self.task_names
                and 'TaskRunner' not in record.name
            ):
                if 'protocol' in self.fs_kwargs:
                    self.fs_kwargs.pop('protocol')

                fs = fsspec.filesystem(
                    protocol=self.fs_protocol, **self.fs_kwargs
                )
                now = datetime.datetime.utcnow()
                logfile = f"{self.bucket_name}/harvest-logs/{record.flow_run_id}__{now:%Y%m%d}.log"
                with fs.open(logfile, mode="a") as f:
                    message = self.__log_formatter.format(record)
                    f.write(f"{message}\n")


# TODO: Break up state handler by task?
# def get_main_flow_state_handler():
#     """Get the main finished flow state handler"""

#     def fn(obj, state):
#         now = datetime.datetime.utcnow().isoformat()
#         flow_run_id = prefect.context.get("flow_run_id")
#         for task in obj.tasks:
#             task_name = task.name
#             task_state = state.result[task]
#             result = task_state.result
#             if task_state.is_failed():
#                 exc_dict = parse_exception(result)
#                 issue = get_issue(flow_run_id, task_name, exc_dict, now)
#                 print(now, flow_run_id, task_name, task_state)
#             elif task_state.is_successful():
#                 print(now, flow_run_id, task_name, task_state)

#     def check(state):
#         return state.is_finished()

#     return callback_factory(fn, check)
