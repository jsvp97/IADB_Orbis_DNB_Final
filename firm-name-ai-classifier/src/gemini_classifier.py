"""
gemini_classifier.py

Classifies company name pairs using the Google Gemini API.

Reads a CSV with columns [name_aff, matched_name] and adds:
  Q1 — "Yes" or "Non"
  Q2 — confidence score 1-10

Usage:
  1. Set INPUT_CSV in src/config.py to your file
  2. Set GEMINI_API_KEY as an environment variable
  3. Run: python src/gemini_classifier.py

The script saves results after every mini-batch, so if it gets interrupted
you can resume by checking how far it got and adjusting the start index.

Author: Sebastian Velasquez (IADB)
"""

import os
import sys
import pandas as pd
import numpy as np
import time
import google.generativeai as genai
from concurrent.futures import ThreadPoolExecutor

# Load config and shared utilities
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import (GEMINI_API_KEY, GEMINI_MODEL, INPUT_CSV, OUTPUT_CSV,
                    NUM_SPLITS, MAX_WORKERS, SLEEP_BETWEEN_BATCHES)
from utils import build_classification_prompt, parse_response

# Set up Gemini
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel(model_name=GEMINI_MODEL)

# Make sure output folder exists (relative to where the script is run from)
output_dir = os.path.dirname(OUTPUT_CSV)
if output_dir:
    os.makedirs(output_dir, exist_ok=True)


# ===========================================================================
# CLASSIFIER FUNCTION
# ===========================================================================

def classify_pair(name_aff: str, matched_name: str) -> tuple:
    """
    Sends one pair to Gemini and returns (Q1, Q2).
    Handles rate limit errors (HTTP 429) by waiting and retrying.
    """
    prompt = build_classification_prompt(name_aff, matched_name)
    while True:
        try:
            response = model.generate_content(prompt)
            return parse_response(response.text.strip())
        except Exception as e:
            if "429" in str(e):
                print("Rate limit reached. Waiting 60 seconds before retrying...")
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
print(f"Model: {GEMINI_MODEL}  |  Workers: {MAX_WORKERS}  |  Splits: {NUM_SPLITS}")

df_splits = np.array_split(df, NUM_SPLITS)
df_result = pd.DataFrame()

for i, df_part in enumerate(df_splits):
    df_part = df_part.copy()
    pairs = list(zip(df_part["name_aff"], df_part["matched_name"]))

    # Send mini-batch in parallel
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
