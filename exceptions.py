from fastapi import Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from schemas import ApiError, ApiErrorResponse

class AppProductionException(Exception):
    def __init__(self, error_code: str, message: str, status_code: int = 400):
        self.error_code = error_code
        self.message = message
        self.status_code = status_code

async def app_exception_handler(request: Request, exc: AppProductionException):
    return build_error_response(
        status_code=exc.status_code,
        code=exc.error_code,
        message=exc.message,
    )

async def request_validation_exception_handler(request: Request, exc: RequestValidationError):
    return build_error_response(
        status_code=422,
        code="VALIDATION_ERROR",
        message="Requete invalide.",
        details=exc.errors(),
    )

async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    if exc.status_code == 404:
        code = "NOT_FOUND"
    elif exc.status_code == 503:
        code = "SERVICE_UNAVAILABLE"
    elif exc.status_code in {401, 403}:
        code = "UNAUTHORIZED"
    else:
        code = "HTTP_ERROR"

    detail = exc.detail if isinstance(exc.detail, str) else "Erreur HTTP."
    details = None if isinstance(exc.detail, str) else exc.detail

    return build_error_response(
        status_code=exc.status_code,
        code=code,
        message=detail,
        details=details,
    )

def build_error_response(
    status_code: int,
    code: str,
    message: str,
    details=None,
):
    payload = ApiErrorResponse(
        error=ApiError(
            code=code,
            message=message,
            details=details,
        )
    ).model_dump(exclude_none=True)

    return JSONResponse(
        status_code=status_code,
        content=payload,
    )
