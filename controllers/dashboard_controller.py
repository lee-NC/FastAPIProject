import traceback
from datetime import datetime

from fastapi import APIRouter

from services.cert_order_service import CertOrderService
from services.credential_service import CredentialService
from services.signature_transaction_service import SignatureTransactionService
from services.user_service import UserService
from model.request_dto import DashboardRequest
from model.response_dto import ResponseModel
from model.response_dto import DashboardResponse
import logging

logger = logging.getLogger("Lakehouse")

router = APIRouter(prefix="/dashboard", tags=["Dashboard"])
credential_service = CredentialService()
user_service = UserService()
cert_order_service = CertOrderService()
signature_transaction_service = SignatureTransactionService()


# region user

@router.post("/count_total_user_active")
def count_total_user_active(request: DashboardRequest):
    try:
        logger.info(f"count_total_user_active at {datetime.now()}")
        mess = user_service.count_total_user_active(request.end_date, request.locality)
        logger.info(f"count_total_user_active success at {datetime.now()} {mess}")
        if mess is None:
            mess = 0
        res = DashboardResponse(quantity=mess)
        return ResponseModel.success(content=res)
    except Exception as e:
        logger.error(traceback.format_exc())
        return ResponseModel.error(str(e))


@router.post("/count_new_user_active")
def count_new_user_active(request: DashboardRequest):
    try:
        logger.info(f"count_new_user_active at {datetime.now()}")
        mess = user_service.count_new_user_active(request.start_date, request.end_date, request.locality)
        logger.info(f"count_new_user_active success at {datetime.now()} {mess}")
        if mess is None:
            mess = 0
        res = DashboardResponse(quantity=mess)
        return ResponseModel.success(content=res)
    except Exception as e:
        logger.error(traceback.format_exc())
        return ResponseModel.error(str(e))


# endregion

# region order
@router.post("/count_total_order")
def count_total_order(request: DashboardRequest):
    try:
        logger.info(f"count_total_order at {datetime.now()}")
        mess = cert_order_service.count_total_cert_order(request.end_date, request.locality)
        logger.info(f"count_total_order success at {datetime.now()} {mess}")
        if mess is None:
            mess = 0
        res = DashboardResponse(quantity=mess)
        return ResponseModel.success(content=res)
    except Exception as e:
        logger.error(traceback.format_exc())
        return ResponseModel.error(str(e))


@router.post("/count_new_order")
def count_new_order(request: DashboardRequest):
    try:
        logger.info(f"count_new_order at {datetime.now()}")
        mess = cert_order_service.count_new_cert_order(request.start_date, request.end_date, request.locality)
        logger.info(f"count_new_order success at {datetime.now()} {mess}")
        if mess is None:
            mess = 0
        res = DashboardResponse(quantity=mess)
        return ResponseModel.success(content=res)
    except Exception as e:
        logger.error(traceback.format_exc())
        return ResponseModel.error(str(e))


# endregion

# region cert
@router.post("/count_total_credential")
def count_total_credential(request: DashboardRequest):
    try:
        logger.info(f"count_total_credential at {datetime.now()}")
        mess = credential_service.count_total_credential(request.end_date, request.locality)
        logger.info(f"count_total_credential success at {datetime.now()} {mess}")
        if mess is None:
            mess = 0
        res = DashboardResponse(quantity=mess)
        return ResponseModel.success(content=res)
    except Exception as e:
        logger.error(traceback.format_exc())
        return ResponseModel.error(str(e))


@router.post("/count_new_credential")
def count_new_credential(request: DashboardRequest):
    try:
        logger.info(f"count_new_credential at {datetime.now()}")
        mess = credential_service.count_new_credential(request.start_date, request.end_date, request.locality)
        logger.info(f"count_new_credential success at {datetime.now()}: {mess}")
        if mess is None:
            mess = 0
        res = DashboardResponse(quantity=mess)
        return ResponseModel.success(content=res)
    except Exception as e:
        logger.error(traceback.format_exc())
        return ResponseModel.error(str(e))


# endregion

# region signature_transaction

@router.post("/count_signature_transaction_by_time")
def count_signature_transaction_by_time(request: DashboardRequest):
    try:
        logger.info(f"count_signature_transaction_by_time at {datetime.now()}")
        mess = signature_transaction_service.count_total_signature_transaction(request.start_date,
                                                                                     request.end_date, request.by_type)
        logger.info(f"count_signature_transaction_by_time success at {datetime.now()} {mess}")
        if mess is None:
            return ResponseModel.not_found()
        return ResponseModel.success(content=mess)
    except Exception as e:
        logger.error(traceback.format_exc())
        return ResponseModel.error(str(e))

# endregion
