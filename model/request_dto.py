import datetime
from typing import Optional

from pydantic import BaseModel


class ReportRequest(BaseModel):
    date: datetime.datetime


class CutOffRequest(BaseModel):
    table_name: str


class DashboardRequest(BaseModel):
    start_date: Optional[datetime.datetime] = None
    end_date: Optional[datetime.datetime] = None
    locality: Optional[str] = None
    account_type: Optional[str] = None
    source: Optional[str] = None
    by_type: Optional[bool] = False
