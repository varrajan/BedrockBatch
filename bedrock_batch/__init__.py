"""bedrock_batch — production framework for Amazon Bedrock batch inference.

Reusable building blocks for any pipeline that uses Bedrock batch:
credential handling, idempotent job submission, EventBridge-based
state-change delivery, and on-demand recovery for jobs that time out.

Public surface:
    from bedrock_batch import (
        make_session, print_identity,
        submit_job, poll_job, get_job_status, ensure_role, ensure_bucket,
        ensure_event_pipeline, consume_events,
        TERMINAL_STATUSES, RECOVERABLE_STATUSES, EVENT_PATTERN,
        recover_job,
    )

See the module docstrings in `aws.py`, `jobs.py`, `events.py`, and
`recovery.py` for the full API. `storage.py` is a reference SQLite
implementation; swap it out for your own observability stack in prod.
"""

from .aws import make_session, print_identity
from .events import (
    EVENT_PATTERN,
    RECOVERABLE_STATUSES,
    TERMINAL_STATUSES,
    consume_events,
    ensure_event_pipeline,
)
from .jobs import (
    count_input_records,
    ensure_bucket,
    ensure_role,
    get_job_progress,
    get_job_status,
    poll_job,
    submit_job,
)
from .recovery import recover_job

__all__ = [
    "EVENT_PATTERN",
    "RECOVERABLE_STATUSES",
    "TERMINAL_STATUSES",
    "consume_events",
    "count_input_records",
    "ensure_bucket",
    "ensure_event_pipeline",
    "ensure_role",
    "get_job_progress",
    "get_job_status",
    "make_session",
    "poll_job",
    "print_identity",
    "recover_job",
    "submit_job",
]
