from types import SimpleNamespace

from litoral_trace.web.us_lacey_operational_views import processing_view


def _detail(*, status="OPEN", documents=()):
    return SimpleNamespace(status=status, documents=documents)


def _document(*, job_status=None, processing_status="STORED"):
    return SimpleNamespace(job_status=job_status, processing_status=processing_status)


def test_processing_progress_uses_durable_job_checkpoints_not_elapsed_time():
    assert processing_view(_detail(documents=(_document(job_status="QUEUED"),))).percent == 35
    assert processing_view(_detail(documents=(_document(job_status="RUNNING"),))).percent == 60


def test_processing_failure_is_terminal_and_customer_safe():
    progress = processing_view(_detail(documents=(_document(job_status="FAILED"),)))
    assert (progress.percent, progress.terminal, progress.failed) == (100, True, True)
    assert "traceback" not in progress.message.casefold()


def test_completed_operation_is_terminal_without_waiting_for_optional_shadow_work():
    progress = processing_view(_detail(status="COMPLETED", documents=(_document(),)))
    assert (progress.percent, progress.state, progress.terminal) == (100, "COMPLETED", True)
