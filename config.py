"""
config.py - Centralized configuration for claude-usage.

Override defaults with environment variables:
  CLAUDE_DIR           - base directory (default: ~/.claude)
  CLAUDE_USAGE_DB      - path to SQLite database
  CLAUDE_PROJECTS_DIR  - path to Claude Code projects directory
"""

import os
from pathlib import Path

_default_claude_dir = Path.home() / ".claude"

CLAUDE_DIR   = Path(os.environ.get("CLAUDE_DIR",          str(_default_claude_dir)))
DB_PATH      = Path(os.environ.get("CLAUDE_USAGE_DB",     str(CLAUDE_DIR / "usage.db")))
PROJECTS_DIR = Path(os.environ.get("CLAUDE_PROJECTS_DIR", str(CLAUDE_DIR / "projects")))
