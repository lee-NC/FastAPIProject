import datetime

from pydantic import BaseModel


class ReportRequest(BaseModel):
    date: datetime


class CutOffRequest(BaseModel):
    table_name: str
