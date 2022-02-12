import logging
from logging import LogRecord
import datetime
from typing import Any, Dict, List
import fsspec


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
