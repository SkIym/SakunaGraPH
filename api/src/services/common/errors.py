class ServiceError(Exception):
    def __init__(
        self,
        status_code: int,
        detail: str,
        *,
        code: str | None = None,
    ) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail
        self.code = code
