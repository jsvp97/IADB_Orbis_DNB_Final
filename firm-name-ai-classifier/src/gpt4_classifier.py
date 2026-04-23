"""
gpt4_classifier.py

Classifies company name pairs using the OpenAI GPT-4 API.

Same logic as gemini_classifier.py — use this when:
  - You have OpenAI credits and want to compare results
  - Gemini is hitting rate limits or returning poor results
  - You prefer GPT-4's behavior on English company names

Reads a CSV with columns [name_aff, matched_name] and adds:
  Q1 — "Yes" or "Non"
  Q2 — confidence score 1-10

Usage:
  1. Set INPUT_CSV and OUTPUT_CSV below to your file paths
  2. Set OPENAI_API_KEY as an environment variable:
       Windows:  set OPENAI_API_KEY=your-key-here
       Mac/Linux: export OPENAI_API_KEY=your-key-here
  3. Run: python src/gpt4_classifier.py

Author: Sebastian Velasquez (IADB)
"""

import os
import pandas as pd
import numpy as np
import time
from openai import OpenAI
from concurrent.futures import ThreadPoolExecutor


# ===========================================================================
# CONFIG — edit these before running
# ===========================================================================

# API key — loaded from environment variable (never hardcode keys in source)
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")

# Model — "gpt-4o-mini" is fast and cheap
# Use "gpt-4o" for higher quality on hard cases
OPENAI_MODEL = "gpt-4o-mini"

# Path to your input CSV (required columns: name_aff, matched_name)
INPUT_CSV = "examples/sample_pairs.csv"

# Where to save the results
OUTPUT_CSV = "output/results.csv"

# How many mini-batches to split the CSV into
NUM_SPLITS = 100

# Parallel API threads — keep at 2-5 to avoid rate limits
MAX_WORKERS = 3

# Seconds to wait between batches
SLEEP_BETWEEN_BATCHES = 5.0


# ===========================================================================
# PROMPT
# ===========================================================================

def build_prompt(name_aff: str, matched_name: str) -> str:
    """
    Builds the classification prompt for a single company name pair.

    We ask the model two questions:
      Q1: Are these the same company or in the same parent group? (Yes / Non)
      Q2: How confident are you? (1-10)

    Design decisions:
    - "Non" instead of "No" keeps output consistent with the Stata pipeline downstream.
    - We tell the model to focus on distinctive tokens — "services", "group",
      "holdings" should not drive a match; unique tokens like "Petrobras" should.
    - Temperature = 0 for deterministic, consistent output.
    - If uncertain, return Non. False positives are harder to fix in research context.
    """
    return f"""
You are a research assistant who specializes in identifying companies.

I will give you two company names. Your job is to decide whether they refer
to the same company, or to companies that belong to the same corporate group
(e.g., a subsidiary and its parent, or two divisions of the same conglomerate).

Always respond in this exact format — nothing else:
Q1: Yes/Non || Q2: <score from 1 to 10>

Rules:
- Q1 = "Yes"  if same company or same parent group
- Q1 = "Non"  if different companies with no ownership connection
- Q2 = your certainty: 1 (very unsure) to 10 (very sure)
- If the names are identical after removing punctuation and case, return Yes with Q2 = 10
- Focus on distinctive words — ignore generic terms like "services", "group",
  "holdings", "international", "global", "solutions"
- Count parent-subsidiary ties as Yes (e.g., "YouTube" and "Google" -> Yes)
- If you're not sure, return Non — it's better to miss a match than to create a false one
- Accept names in any language (Spanish, English, Portuguese, etc.)

name_aff: {name_aff}
matched_name: {matched_name}
""".strip()


# ===========================================================================
# RESPONSE PARSER
# ===========================================================================

def parse_response(raw_response: str) -> tuple:
    """
    Parses the model's response into (Q1, Q2).

    Expected format: "Q1: Yes/Non || Q2: 8"

    Returns (raw_response, "") if the format is unexpected,
    so unexpected outputs are visible for manual inspection.
    """
    if "||" in raw_response:
        try:
            parts = raw_response.split("||")
            q1 = parts[0].replace("Q1:", "").strip()
            q2 = parts[1].replace("Q2:", "").strip()
            return q1, q2
        except Exception:
            pass
    return raw_response.strip(), ""


# ===========================================================================
# SETUP
# ===========================================================================

if not OPENAI_API_KEY:
    print("WARNING: OPENAI_API_KEY is not set. Set it as an environment variable.")

client = OpenAI(api_key=OPENAI_API_KEY)

# Create output folder if it doesn't exist
output_dir = os.path.dirname(OUTPUT_CSV)
if output_dir:
    os.makedirs(output_dir, exist_ok=True)


# ===========================================================================
# CLASSIFIER FUNCTION
# ===========================================================================

def classify_pair(name_aff: str, matched_name: str) -> tuple:
    """
    Sends one pair to GPT-4o-mini and returns (Q1, Q2).
    Temperature is set to 0 for consistent, deterministic output.
    """
    prompt = build_prompt(name_aff, matched_name)
    while True:
        try:
            response = client.responses.create(
                model=OPENAI_MODEL,
                input=prompt,
                temperature=0
            )
            return parse_response(response.output_text.strip())
        except Exception as e:
            if "429" in str(e):
                print("Rate limit reached. Waiting 60 seconds...")
                time.sleep(60)
                continue
            else:
                print(f"Error on pair ({name_aff}, {matched_name}): {e}")
                return "ERROR", ""


# ===========================================================================
# MAIN
# ===========================================================================

df = pd.read_csv(INPUT_CSV)

if "name_aff" not in df.columns or "matched_name" not in df.columns:
    raise ValueError("Input CSV must have columns: name_aff, matched_name")

print(f"Loaded {len(df):,} pairs from {INPUT_CSV}")
print(f"Model: {OPENAI_MODEL}  |  Workers: {MAX_WORKERS}  |  Splits: {NUM_SPLITS}")

df_splits = np.array_split(df, NUM_SPLITS)
df_result = pd.DataFrame()

for i, df_part in enumerate(df_splits):
    df_part = df_part.copy()
    pairs = list(zip(df_part["name_aff"], df_part["matched_name"]))

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(lambda p: classify_pair(p[0], p[1]), pairs))

    df_part["Q1"] = [r[0] for r in results]
    df_part["Q2"] = [r[1] for r in results]

    df_result = pd.concat([df_result, df_part])
    print(f"Batch {i+1}/{NUM_SPLITS} done  ({len(df_result):,} rows saved so far)")

    # Save incrementally — if the run breaks, you won't lose everything
    df_result.to_csv(OUTPUT_CSV, index=False)
    time.sleep(SLEEP_BETWEEN_BATCHES)

print(f"\nAll done. Results saved to: {OUTPUT_CSV}")
print(f"  Yes: {(df_result['Q1']=='Yes').sum():,}  |  Non: {(df_result['Q1']=='Non').sum():,}")
