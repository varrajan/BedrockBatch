"""Shared boto3 session + identity check.

Centralizes credential selection so every caller uses the same session.
By default we rely on the standard boto3 credential chain (env vars,
~/.aws/credentials, SSO, instance role). A `profile_name` forces a
specific profile.
"""

from __future__ import annotations

import sys


def make_session(profile_name: str | None = None):
    """Return a boto3 Session, honoring an optional profile name."""
    import boto3

    if profile_name:
        return boto3.Session(profile_name=profile_name)
    return boto3.Session()


def print_identity(session, label: str = "") -> dict:
    """Call STS GetCallerIdentity and print account/arn.

    Fails loudly (SystemExit 2) if credentials are missing or expired,
    so a wrong-account submission is impossible to miss.
    """
    try:
        sts = session.client("sts")
        ident = sts.get_caller_identity()
    except Exception as e:
        print(f"ERROR: could not resolve AWS credentials ({label}): {e}", file=sys.stderr)
        raise SystemExit(2)

    tag = f" [{label}]" if label else ""
    print(f"AWS identity{tag}: account={ident['Account']} arn={ident['Arn']}")
    return ident
