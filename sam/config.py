from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

APP_NAME = os.getenv("APP_NAME", "Survey Assignment Manager").strip() or "Survey Assignment Manager"
DEFAULT_MIN_HOURS = float(os.getenv("DEFAULT_MIN_HOURS", "6.5"))
DEFAULT_MAX_HOURS = float(os.getenv("DEFAULT_MAX_HOURS", "9"))
DEFAULT_HUB = os.getenv("DEFAULT_HUB", "Charlotte Transportation Center").strip()
MIN_SWITCH_MINUTES = int(os.getenv("MIN_SWITCH_MINUTES", "10"))
MAX_SWITCH_MINUTES = int(os.getenv("MAX_SWITCH_MINUTES", "90"))
