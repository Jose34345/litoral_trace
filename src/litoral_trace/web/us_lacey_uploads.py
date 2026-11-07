"""Bound document reads before the portal creates or updates a shipment."""

from starlette.datastructures import UploadFile

from litoral_trace.us_lacey.storage import us_lacey_max_request_bytes, us_lacey_max_upload_bytes
from litoral_trace.us_lacey.workflow import UsLaceyWorkflowError


async def read_operation_uploads(
    uploads: list[UploadFile], *, document_role: str
) -> tuple[tuple[str, str, bytes, str], ...]:
    limit = us_lacey_max_upload_bytes()
    too_large = "The uploaded document exceeds the maximum file size."
    total_limit = us_lacey_max_request_bytes()
    total_too_large = "The uploaded request exceeds the maximum total upload size."
    known_total = sum(upload.size or 0 for upload in uploads)
    if known_total > total_limit:
        raise UsLaceyWorkflowError(total_too_large)
    # Starlette measures each part while parsing multipart. Check every part
    # before reading any payload so a large later file cannot waste earlier reads.
    if any(upload.size is not None and upload.size > limit for upload in uploads):
        raise UsLaceyWorkflowError(too_large)

    payloads = []
    total_read = 0
    for upload in uploads:
        content = await upload.read(limit + 1)
        if not content:
            raise UsLaceyWorkflowError("The uploaded document is empty.")
        if len(content) > limit:
            raise UsLaceyWorkflowError(too_large)
        total_read += len(content)
        if total_read > total_limit:
            raise UsLaceyWorkflowError(total_too_large)
        payloads.append((
            upload.filename or "document",
            upload.content_type or "application/octet-stream",
            content,
            document_role,
        ))
    return tuple(payloads)
