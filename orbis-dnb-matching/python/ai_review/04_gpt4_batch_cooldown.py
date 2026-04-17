"""
04_gpt4_batch_cooldown.py

Same as 03_gemini_batch_cooldown.py but uses the OpenAI API (GPT-4o-mini).
Output format is identical so 07_final_match_pipeline.do processes both the same way.

Input:  a preIA chunk CSV from 04_ai_review_prep.do (columns: name_aff, matched_name)
Output: same CSV with Q1 ("Yes"/"Non") and Q2 (score 1-10) columns added

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
from config import OPENAI_API_KEY, WORK_DIR, OPENAI_MODEL, NUM_SPLITS, MAX_WORKERS

client = OpenAI(api_key=OPENAI_API_KEY)

# ===========================================================================
# CONFIG — set INPUT_CSV to the chunk you want to run
# ===========================================================================

os.chdir(WORK_DIR)
INPUT_CSV  = 'fuzzy_match_v1_preIA_0_170000.csv'      # <-- change per chunk
OUTPUT_CSV = INPUT_CSV.replace('preIA', 'postIA_gpt4').replace('.csv', '_reviewed.csv')

df_complete = pd.read_csv(INPUT_CSV)


# ===========================================================================
# PROMPT
# ===========================================================================

def build_prompt(name_aff, matched_name):
    return f"""
    You are a research assistant specialized in identifying companies.
    I will give you two company names: one is the original company name (name_aff),
    and the other is the result of a previous database match (matched_name).

    Your task: determine whether name_aff and matched_name refer to the same company
    or companies belonging to the same parent company.

    Output format (always exactly this, no explanations):
    Q1: Yes/Non || Q2: <score from 1 to 10>

    Q1 rules:
    - "Yes" if both are the same company or share the same ultimate parent
    - "Non" otherwise

    Q2 rules:
    - 1 = very low certainty, 10 = very high certainty

    Additional guidelines:
    1. Use your knowledge of subsidiaries, M&A, and corporate groups
    2. Automatically mark identical names (after normalization) as Yes with score 10
    3. Focus on distinctive tokens — ignore generic words like "health", "logistics",
       "services", "group", "holdings" when comparing names
    4. Count ties to the same parent as Yes
    5. If uncertain, return Non (false positives are more harmful than misses)
    6. Accept Spanish and English company names equally

    name_aff: {name_aff}
    matched_name: {matched_name}
    """


# ===========================================================================
# API CALL
# ===========================================================================

def ask_openai(name_aff, matched_name):
    """Calls GPT-4o-mini for one pair. Retries after 60s on rate limit."""
    prompt = build_prompt(name_aff, matched_name)
    while True:
        try:
            response = client.responses.create(
                model=OPENAI_MODEL,
                input=prompt,
                temperature=0
            )
            output_text = response.output_text.strip()

            if "||" in output_text:
                q1, q2 = [
                    x.strip().replace("Q1:", "").replace("Q2:", "").strip()
                    for x in output_text.split("||")
                ]
                return q1, q2
            else:
                return output_text, ""

        except Exception as e:
            if "429" in str(e):
                print("Rate limit hit. Waiting 60 seconds before retrying...")
                time.sleep(60)
                continue
            else:
                print(f"Unexpected error: {e}")
                return "ERROR", ""


# ===========================================================================
# BATCH PROCESSING
# ===========================================================================

df_splits = np.array_split(df_complete, NUM_SPLITS)
df_result = pd.DataFrame()

for i in range(NUM_SPLITS):
    df_part = df_splits[i].copy()
    pairs = list(zip(df_part['name_aff'], df_part['matched_name']))

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(lambda p: ask_openai(p[0], p[1]), pairs))

    df_part['Q1'] = [r[0] for r in results]
    df_part['Q2'] = [r[1] for r in results]

    df_result = pd.concat([df_result, df_part])
    print(f'Finished batch {i+1}/{NUM_SPLITS}')

    df_result.to_csv(OUTPUT_CSV, index=False)
    time.sleep(5)

print(f"Done. Results saved to {OUTPUT_CSV}")
