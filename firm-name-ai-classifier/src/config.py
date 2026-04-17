"""
config.py — firm-name-ai-classifier

All settings in one place. Edit this file to point to your data
and set your API keys.

IMPORTANT: Never paste real API keys into this file directly.
Use environment variables or a .env file (see .env.example).
"""

import os
from pathlib import Path

# Try to load from .env file if it exists
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass   # python-dotenv not installed, that's fine — use env vars instead


# ===========================================================================
# API KEYS
# ===========================================================================

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")

if not GEMINI_API_KEY:
    print("WARNING: GEMINI_API_KEY not set. Set it as an environment variable.")
if not OPENAI_API_KEY:
    print("WARNING: OPENAI_API_KEY not set. Set it as an environment variable.")


# ===========================================================================
# INPUT / OUTPUT
# ===========================================================================

# Path to your input CSV.
# Required columns: name_aff, matched_name
INPUT_CSV = "examples/sample_pairs.csv"

# Where to save the results (same CSV with Q1 and Q2 columns added)
OUTPUT_CSV = "output/results.csv"


# ===========================================================================
# MODEL SETTINGS
# ===========================================================================

# Gemini model — "gemini-2.0-flash" is fast and cheap
# Use "gemini-2.5-pro" for higher quality on tricky cases
GEMINI_MODEL = "gemini-2.0-flash"

# OpenAI model — "gpt-4o-mini" is fast and cheap
# Use "gpt-4o" for higher quality
OPENAI_MODEL = "gpt-4o-mini"


# ===========================================================================
# BATCH SETTINGS
# ===========================================================================

# How many mini-batches to split the input CSV into.
# Each batch is processed independently so you can resume after failures.
# Larger values = smaller batches = easier to resume, but slightly slower.
NUM_SPLITS = 100

# Number of parallel API calls per batch.
# Keep this at 2-5 to avoid rate limits. Increase if you have a paid plan.
MAX_WORKERS = 3

# Seconds to wait between batches. Increase if you hit 429 errors frequently.
SLEEP_BETWEEN_BATCHES = 5.0
