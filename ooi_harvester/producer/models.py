from typing import List

from pydantic import BaseModel, validator


class Stream(BaseModel):
    method: str
    name: str

    @validator('*')
    def must_exists(cls, v):
        if not v.strip():
            raise ValueError('Stream method or name cannot be empty')
        return v


class HarvestOptions(BaseModel):
    refresh: bool
    test: bool
    path: str

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
