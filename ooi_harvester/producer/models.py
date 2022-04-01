from typing import List, Literal, Optional, Dict, Any
from datetime import datetime
from copy import deepcopy

from pydantic import BaseModel, validator


class Stream(BaseModel):
    method: str
    name: str

    @validator('*')
    def must_exists(cls, v):
        if not v.strip():
            raise ValueError('Stream method or name cannot be empty')
        return v


class HarvestRange(BaseModel):
    start: Optional[str]
    end: Optional[str]

    @validator('start')
    def start_isoformat(cls, v):
        try:
            if v:
                datetime.fromisoformat(v)
            return v
        except Exception:
            raise ValueError(
                'start custom range is not in ISO 8601 format (yyyy-MM-ddTHH:mm:ss.SSS)'
            )

    @validator('end')
    def end_isoformat(cls, v):
        try:
            if v:
                datetime.fromisoformat(v)
            return v
        except Exception:
            raise ValueError(
                'end custom range is not in ISO 8601 format (yyyy-MM-ddTHH:mm:ss.SSSZ)'
            )


class HarvestOptions(BaseModel):
    path: str
    refresh: bool = False
    test: bool = False
    goldcopy: bool = False
    path_settings: dict = {}
    custom_range: HarvestRange = HarvestRange()

    @validator('path')
    def path_must_exists(cls, v):
        if not v.strip():
            raise ValueError('Harvest destination path cannot be empty')
        return v


class Workflow(BaseModel):
    schedule: str


class HarvestStatus(BaseModel):
    status: Literal[
        "started", "pending", "failed", "success", "discontinued", "unknown"
    ] = "unknown"
    last_updated: Optional[str]
    # OOI Data request information
    data_ready: bool = False
    data_response: Optional[str]
    requested_at: Optional[str]
    # Cloud data information
    process_status: Optional[Literal["pending", "failed", "success"]]
    cloud_location: Optional[str]
    start_date: Optional[str]
    end_date: Optional[str]
    processed_at: Optional[str]
    last_refresh: Optional[str]
    data_check: bool = False

    def __init__(self, **data):
        super().__init__(**data)
        self.last_updated = datetime.utcnow().isoformat()

    @validator('processed_at')
    def processed_at_isoformat(cls, v):
        try:
            if v:
                datetime.fromisoformat(v)
            return v
        except Exception:
            raise ValueError(
                'processed_at is not in ISO 8601 format (yyyy-MM-ddTHH:mm:ss.SSS)'
            )

    @validator('last_refresh')
    def last_refresh_isoformat(cls, v):
        try:
            if v:
                datetime.fromisoformat(v)
            return v
        except Exception:
            raise ValueError(
                'last_refresh is not in ISO 8601 format (yyyy-MM-ddTHH:mm:ss.SSS)'
            )

    @validator('requested_at')
    def requested_at_isoformat(cls, v):
        try:
            if v:
                datetime.fromisoformat(v)
            return v
        except Exception:
            raise ValueError(
                'requested_at is not in ISO 8601 format (yyyy-MM-ddTHH:mm:ss.SSS)'
            )


class StreamHarvest(BaseModel):
    instrument: str
    stream: Stream
    assignees: Optional[List[str]]
    labels: Optional[List[str]]
    harvest_options: HarvestOptions
    workflow_config: Workflow
    table_name: Optional[str]
    _status: HarvestStatus

    def __init__(self, **data):
        super().__init__(**data)
        self._status = HarvestStatus()

    @property
    def status(self):
        return self._status

    def update_status(self, status_input: Dict[str, Any]):
        new_status = deepcopy(self._status.dict())
        new_status.update(status_input)
        self._status = HarvestStatus(**new_status)

    @validator('instrument')
    def instrument_must_exists(cls, v):
        if not v.strip():
            raise ValueError('Instrument cannot be empty')
        return v

    @validator("table_name", always=True)
    def set_table_name(cls, v, values):
        if 'instrument' in values and 'stream' in values:
            return "-".join(
                [
                    values['instrument'],
                    values['stream'].method,
                    values['stream'].name,
                ]
            )

    class Config:
        underscore_attrs_are_private = True
