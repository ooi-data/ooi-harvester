from typing import List
from datetime import datetime

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
    start: str = None
    end: str = None

    @validator('start')
    def start_isoformat(cls, v):
        try:
            if v:
                datetime.fromisoformat(v)
            return v
        except Exception:
            raise ValueError(
                'start custom range is not in ISO 8601 format (yyyy-MM-ddTHH:mm:ss.SSSZ)'
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
    refresh: bool
    test: bool
    path: str
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


class StreamHarvest(BaseModel):
    instrument: str
    stream: Stream
    assigness: List[str]
    harvest_options: HarvestOptions
    workflow_config: Workflow
    table_name: str = None

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
