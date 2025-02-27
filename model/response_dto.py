from datetime import datetime

import pandas as pd
from pydantic import BaseModel
from typing import Optional, Any


class ResponseModel(BaseModel):
    code: int
    codeDesc: str
    message: Optional[str] = ""
    content: Optional[Any] = None

    @classmethod
    def success(cls, mess: str = "", content: Optional[Any] = None):
        """ Trả về response thành công """
        if isinstance(content, pd.DataFrame):
            content = content.to_dict(orient="records")
        return cls(code=0, codeDesc="SUCCESS", message=mess, content=content)

    @classmethod
    def bad_request(cls, message: str = "Request không hợp lệ"):
        """ Trả về response khi request không hợp lệ """
        return cls(code=400, codeDesc="BAD_REQUEST", message=message, content=None)

    @classmethod
    def not_found(cls, message: str = "Không tìm thấy thông tin"):
        """ Không tìm thấy thông tin """
        return cls(code=404, codeDesc="NOT_FOUND", message=message, content=None)

    @classmethod
    def timeout(cls, message: str = "Chờ quá lâu, vui lòng thử lại"):
        """ Trả về response khi request quá thời gian """
        return cls(code=499, codeDesc="TIMEOUT", message=message, content=None)

    @classmethod
    def error(cls, message: str = "Lỗi hệ thống"):
        """ Trả về response khi request quá thời gian """
        return cls(code=500, codeDesc="SERVER_ERROR", message=message, content=None)


class DashboardResponse(BaseModel):
    app_name: str = ""
    quantity: int = 0
    locality: str = ""
    account_type: str = ""
    source: str = ""
    month: str = ""
    day: str = ""
