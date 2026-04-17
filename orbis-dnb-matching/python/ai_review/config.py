"""
config.py — shared settings for the AI review scripts.

API keys are read from environment variables (never hardcoded).
Set them before running:

  Windows:  set GEMINI_API_KEY=your_key
            set OPENAI_API_KEY=your_key

  Mac/Linux: export GEMINI_API_KEY=your_key
             export OPENAI_API_KEY=your_key

  Or copy .env.example to .env and load with python-dotenv.

Keys at: https://aistudio.google.com/app/apikey  (Gemini)
         https://platform.openai.com/api-keys     (OpenAI)
"""

import os

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")

if not GEMINI_API_KEY:
    print("WARNING: GEMINI_API_KEY is not set.")
if not OPENAI_API_KEY:
    print("WARNING: OPENAI_API_KEY is not set.")

# Change WORK_DIR if you move the project
WORK_DIR = "C:/Sebas BID/Orbis_DNB/IA review/"

GEMINI_MODEL = "gemini-2.0-flash"   # use gemini-2.5-pro for higher quality
OPENAI_MODEL = "gpt-4o-mini"        # use gpt-4o for higher quality

NUM_SPLITS           = 100    # chunks per CSV — smaller = easier to resume
MAX_WORKERS          = 3      # parallel API threads — keep low on free tier
SLEEP_BETWEEN_CHUNKS = 10.0   # seconds between chunks
