"""Single-job child-process entrypoint for U.S. Lacey processing.

This module is intentionally tiny at import time. Heavy document-processing
dependencies are imported only inside the child process so the long-lived
supervisor never accumulates PDF/OCR/AI memory.
"""
from __future__ import annotations

import argparse
import gc
import logging
import os


_LOG = logging.getLogger("litoral_trace.us_lacey.worker_once")
EXIT_OK = 0
EXIT_ERROR = 1
EXIT_NO_JOB = 3


def _malloc_trim() -> None:
    """Best-effort glibc heap trim before process exit.

    Process termination is the real reclamation boundary. This is only defense
    in depth for graceful shutdown paths and has no correctness dependency.
    """
    if os.name != "posix":
        return
    try:
        import ctypes

        libc = ctypes.CDLL(None)
        malloc_trim = getattr(libc, "malloc_trim", None)
        if malloc_trim is not None:
            malloc_trim(0)
    except Exception:
        _LOG.debug("malloc_trim unavailable", exc_info=True)


def run_one(*, worker_id: str) -> int:
    """Claim at most one durable job, process it, and return an exit code."""
    from litoral_trace.us_lacey.worker import process_one_us_lacey_job

    try:
        result = process_one_us_lacey_job(worker_id=worker_id)
        if not result.claimed:
            return EXIT_NO_JOB
        _LOG.info(
            "child_job_processed job_id=%s job_status=%s document_status=%s "
            "operation_status=%s projected=%s conflicts=%s",
            result.job_id,
            result.job_status,
            result.document_status,
            result.operation_status,
            result.projected_count,
            result.conflict_count,
        )
        return EXIT_OK
    except BaseException:
        _LOG.exception("child_worker_iteration_failed worker_id=%s", worker_id)
        return EXIT_ERROR
    finally:
        gc.collect()
        _malloc_trim()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker-id", required=True)
    args = parser.parse_args()
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    return run_one(worker_id=str(args.worker_id))


if __name__ == "__main__":
    raise SystemExit(main())
