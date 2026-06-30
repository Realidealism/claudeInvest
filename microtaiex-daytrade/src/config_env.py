"""Minimal .env loader (no third-party dependency).

Searches the current directory and its ancestors for a ``.env`` file, loading
KEY=VALUE lines into os.environ WITHOUT overwriting existing variables.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional


def load_dotenv() -> Optional[str]:
    cwd = Path.cwd()
    for d in [cwd, *cwd.parents]:
        f = d / ".env"
        if not f.exists():
            continue
        for line in f.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, val = line.split("=", 1)
            os.environ.setdefault(key.strip(), val.strip().strip('"').strip("'"))
        return str(f)
    return None
