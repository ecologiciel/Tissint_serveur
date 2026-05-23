from fastapi import Request
from fastapi.responses import JSONResponse

class AppProductionException(Exception):
    def __init__(self, error_code: str, message: str, status_code: int = 400):
        self.error_code = error_code
        self.message = message
        self.status_code = status_code

async def app_exception_handler(request: Request, exc: AppProductionException):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "status_code": "DIAGNOSTIC_FAILED",
            "error": {
                "code": exc.error_code,
                "message": exc.message
            }
        }
    )
