class LaceyEngineError(ValueError):
    """Raised when a document cannot be parsed safely by the isolated engine."""


UNSUPPORTED_DOCUMENT_DOMAIN_MESSAGE = (
    "This file does not appear to be a commercial invoice, packing list, or "
    "transport document. Litoral Trace cannot process legal or administrative files."
)


class UnsupportedDocumentDomainError(LaceyEngineError):
    """Raised when a physical source is deterministically outside the trade domain."""

    code = "UNSUPPORTED_DOMAIN"

    def __init__(
        self,
        *,
        domain: str = "UNSUPPORTED",
        safe_message: str = UNSUPPORTED_DOCUMENT_DOMAIN_MESSAGE,
    ) -> None:
        self.domain = str(domain or "UNSUPPORTED")
        self.safe_message = str(safe_message)
        super().__init__(self.safe_message)
