"""
03_gemini_batch_cooldown.py

Sends fuzzy match candidate pairs to the Gemini API for review.
For each pair (name_aff, matched_name), the model answers:
  Q1: Are these the same company or parent group?  → "Yes" / "Non"
  Q2: Confidence score 1–10

Rate limits on the free tier are around 15 req/min.
The 10s sleep + 3 workers keep us well below that.
On a paid plan, increase MAX_WORKERS and reduce SLEEP_BETWEEN_CHUNKS.

Input:  a preIA chunk CSV from 04_ai_review_prep.do (columns: name_aff, matched_name)
Output: same CSV with respuesta_gpt4 column added (raw LLM response)

Author: Sebastian Velasquez (IADB)
"""

import os
import sys
import pandas as pd
import numpy as np
import time
import google.generativeai as genai
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import GEMINI_API_KEY, WORK_DIR, GEMINI_MODEL, NUM_SPLITS, MAX_WORKERS, SLEEP_BETWEEN_CHUNKS

genai.configure(api_key=GEMINI_API_KEY)

# ===========================================================================
# CONFIG — set INPUT_CSV to the chunk you want to run
# ===========================================================================

os.chdir(WORK_DIR)
INPUT_CSV  = 'fuzzy_match_v1_preIA_0_170000.csv'     # <-- change per chunk
OUTPUT_CSV = INPUT_CSV.replace('preIA', 'postIA').replace('.csv', '_reviewed.csv')

df_complete = pd.read_csv(INPUT_CSV)
model = genai.GenerativeModel(model_name=GEMINI_MODEL)


# ===========================================================================
# PROMPT
# ===========================================================================

def build_prompt(name_aff, matched_name):
    return f"""
    You are a research assistant specialized in identifying companies.
    I will give you two company names: one is the original company name (name_aff),
    and the other is the result of a previous database match (matched_name).

    Your task: determine whether name_aff and matched_name refer to the same company
    or to companies belonging to the same parent company.

    Always return your answer in this exact format — nothing else:
    Q1: Yes/Non || Q2: <score from 1 to 10>

    Q1 = "Yes"  if both names refer to the same company or share the same parent
    Q1 = "Non"  otherwise

    Q2 = certainty score: 1 (very uncertain) to 10 (very certain)

    Rules:
    - Use your knowledge of subsidiaries, M&A history, and business groups
    - If the names are identical after removing punctuation/case, mark Yes with score 10
    - Focus on distinctive tokens — ignore generic words like "services", "logistics", "group"
    - If uncertain, return Non (err on the side of caution)
    - Do not include any explanation — just the Q1/Q2 line

    name_aff: {name_aff}
    matched_name: {matched_name}
    """


# ===========================================================================
# API CALL
# ===========================================================================

def ask_gemini(name_aff, matched_name):
    """Calls Gemini for one pair. Waits 12 hours on rate limit (HTTP 429)."""
    prompt = build_prompt(name_aff, matched_name)
    while True:
        try:
            response = model.generate_content(prompt)
            return response.text.strip()
        except Exception as e:
            if "429" in str(e):
                print("Rate limit hit. Waiting 12 hours before retrying...")
                time.sleep(43200)
                continue
            else:
                print(f"Unexpected error: {e}")
                return f"ERROR: {e}"


# ===========================================================================
# BATCH PROCESSING
# ===========================================================================

df_splits = np.array_split(df_complete, NUM_SPLITS)
df_result = pd.DataFrame()

for i in range(NUM_SPLITS):
    df_part = df_splits[i]
    pairs = list(zip(df_part['name_aff'], df_part['matched_name']))

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(lambda p: ask_gemini(p[0], p[1]), pairs))

    df_part = df_part.copy()
    df_part['respuesta_gpt4'] = results
    df_result = pd.concat([df_result, df_part])
    print(f'Finished batch {i+1}/{NUM_SPLITS}')

    df_result.to_csv(OUTPUT_CSV, index=False)
    time.sleep(SLEEP_BETWEEN_CHUNKS)

print(f"Done. Results saved to {OUTPUT_CSV}")
