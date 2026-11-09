#!/usr/bin/env python3
"""Print the AWS identity the benchmark will use."""

from __future__ import annotations

import sys
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bedrock_batch import make_session, print_identity


def main() -> int:
    cfg_path = Path(__file__).resolve().parent / "config.yaml"
    with cfg_path.open() as fh:
        cfg = yaml.safe_load(fh)

    print(f"aws_profile from config: {cfg.get('aws_profile') or '(default chain)'}")
    session = make_session(cfg.get("aws_profile"))
    print_identity(session)

    for region in cfg["regions"]:
        print(f"  region={region}: ready")
    return 0


if __name__ == "__main__":
    sys.exit(main())
