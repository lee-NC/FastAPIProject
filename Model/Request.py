import datetime

from pydantic import BaseModel


class ReportRequest(BaseModel):
    date: datetime.datetime


class CutOffRequest(BaseModel):
    table_name: str
