"""
gpt4_classifier.py

Same as gemini_classifier.py but uses the OpenAI API (GPT-4o-mini).

Use this when:
  - You have OpenAI credits and want to compare results
  - Gemini is hitting rate limits or returning poor results
  - You prefer GPT-4's behavior on English company names

Output format is identical to the Gemini version.

Author: Sebastian Velasquez (IADB)
"""

import os
import sys
import pandas as pd
import numpy as np
import time
from openai import OpenAI
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import (OPENAI_API_KEY, OPENAI_MODEL, INPUT_CSV, OUTPUT_CSV,
                    NUM_SPLITS, MAX_WORKERS, SLEEP_BETWEEN_BATCHES)
from utils import build_classification_prompt, parse_response

client = OpenAI(api_key=OPENAI_API_KEY)

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
    prompt = build_classification_prompt(name_aff, matched_name)
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

    df_result.to_csv(OUTPUT_CSV, index=False)
    time.sleep(SLEEP_BETWEEN_BATCHES)

print(f"\nAll done. Results saved to: {OUTPUT_CSV}")
print(f"  Yes: {(df_result['Q1']=='Yes').sum():,}  |  Non: {(df_result['Q1']=='Non').sum():,}")
