"""Benchmark-only: synthetic workloads for the sizing experiment.

A production caller would supply their own records; this file exists
solely so the benchmark has a fixed, repeatable payload to isolate
batch system behavior.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

SHORT_PROMPT = (
    "Classify the sentiment of the following review as POSITIVE, NEGATIVE, or NEUTRAL. "
    "Reply with one word only.\n\nReview: {review}"
)

SHORT_REVIEWS = [
    "The product arrived on time and works great.",
    "Terrible experience, broke on day two.",
    "It's fine, nothing special.",
    "Absolutely love this, would buy again.",
    "Mediocre at best, overpriced for the quality.",
]

MEDIUM_PROMPT = (
    "Summarize the following passage in 2-3 sentences, preserving the main argument.\n\n"
    "Passage: {passage}"
)

MEDIUM_PASSAGE = (
    "Large language models have transformed natural language processing by scaling "
    "transformer architectures to hundreds of billions of parameters. Training these "
    "models requires significant computational resources and curated datasets. Recent "
    "advances in inference optimization, including quantization, speculative decoding, "
    "and batched request serving, have substantially reduced the cost of deploying "
    "these models in production. Batch inference APIs offered by cloud providers "
    "exchange latency for a meaningful discount, making large-scale offline workloads "
    "economically viable."
)


def build_record(workload_kind: str, index: int) -> dict:
    if workload_kind == "short":
        review = SHORT_REVIEWS[index % len(SHORT_REVIEWS)]
        user_text = SHORT_PROMPT.format(review=review)
        max_tokens = 16
    elif workload_kind == "medium":
        user_text = MEDIUM_PROMPT.format(passage=MEDIUM_PASSAGE)
        max_tokens = 200
    else:
        raise ValueError(f"Unknown workload kind: {workload_kind}")

    return {
        "recordId": f"rec-{uuid.uuid4().hex[:12]}-{index:08d}",
        "modelInput": {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": max_tokens,
            "messages": [{"role": "user", "content": user_text}],
        },
    }


def write_jsonl(path: Path, count: int, workload_kind: str) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    total = 0
    with path.open("w") as fh:
        for i in range(count):
            line = json.dumps(build_record(workload_kind, i)) + "\n"
            fh.write(line)
            total += len(line)
    return total
