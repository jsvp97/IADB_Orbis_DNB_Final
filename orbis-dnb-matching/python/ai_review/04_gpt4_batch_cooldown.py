"""
04_gpt4_batch_cooldown.py

Classifies company name pairs using the OpenAI GPT-4 API.

For each candidate pair produced by the fuzzy matching step, the model answers:
  Q1 — "Yes" if both names refer to the same company or share the same parent, "Non" otherwise
  Q2 — confidence score 1–10

The script reads a CSV with columns [name_aff, matched_name] and appends
Q1 and Q2 columns with the parsed model responses.

If you hit a 429 (rate limit), the script waits 1 minute automatically and retries.
Results are saved after every mini-batch so you can resume if interrupted.

The prompt is designed to handle cross-language names, abbreviations, rebrands,
and parent-subsidiary relationships. It focuses on distinctive tokens and ignores
generic industry words (e.g., "services", "logistics", "group").

Set OPENAI_API_KEY as an environment variable before running:
  export OPENAI_API_KEY="your-key-here"   (Linux/Mac)
  set OPENAI_API_KEY=your-key-here        (Windows)

Input:  fuzzy_match_v1_preIA_<start>_<end>.csv  (from 04_ai_review_prep.do)
Output: retviews_openai_final_fuzzy_match_<start>_<end>.csv

Author: Sebastian Velasquez (IADB)
"""

## GPT 4 retviews
## SV 12_08_2025

import os
import pandas as pd
import numpy as np
import time
from openai import OpenAI
from concurrent.futures import ThreadPoolExecutor

# ==== SETUP ====
# Load API key from environment variable — never hardcode keys in source files
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")

client = OpenAI(api_key=OPENAI_API_KEY)

# Working directory and file
os.chdir("C:/Sebas BID/Orbis_DNB/IA review/")
input_csv = 'fuzzy_match_v1_preIA_382500_510000.csv'
df_complete = pd.read_csv(input_csv)

# Number of chunks to split the CSV
num_splits = 100

# ==== PROMPT BUILDER ====
def build_prompt(name_aff, matched_name):
    return f"""

    You are a research assistant specialized in identifying companies.
    I will give you two company names: one is the original company name (name_aff), and the other is the result of a previous database match (matched_name).

    Your task: Determine whether name_aff and matched_name refer to the same company or companies belonging to the same parent company.

    Output format (always in this structure, no explanations):
    Q1: Yes/Non || Q2: <score from 1 to 10>

    Q1 rules:
    - "Yes" if both companies are the same or share the same parent company
    - "Non" otherwise

    Q2 rules:
    - 1 = very low certainty, 10 = very high certainty

    name_aff: {name_aff}
    matched_name: {matched_name}

    Additional instructions:
    1. Use the web to verify parent-company/ownership relationships and company details
    2. Mark Yes with a lower score, do not include any aditional information
    3. Yes apply common normalization rules
    4. Use web verification, but in case it show nothing conclusive use balance fuzzy matching behavior
    5. Output format with two columns added to the CSV, nothing more
    6. The file contains 170,000 rows, take as much time as you need to fulfill this task correctly. If you found any issue with performance let me know and ask me questions
    7. Preferred language is english, but consider spanish too.
    8. Yes, count ties to parent-company ownership counted as a yes
    9. No blacklist or entity tipes to ignore
    10. To proceed efficiently, automatically mark pairs as "Yes" with high confidence when the names are identical (after normalization such as removing punctuation and case differences), and then use web verification and fuzzy matching primarily for the non-identical pairs

    Include checks for distinctive tokens and industry context. In practice, this means the algorithm now disregards generic or industry‑common words (e.g., "health," "logistics," "services," "paradise," "sociedade") when comparing names. Instead, it focuses on unique tokens—those that genuinely identify a company—and only considers a match if those distinctive tokens overlap meaningfully.
    This approach prevents pairs from being matched simply because they share common descriptors.

    """

# ==== FUNCTION TO CALL OPENAI ====
def ask_openai(name_aff, matched_name):
    prompt = build_prompt(name_aff, matched_name)
    while True:
        try:
            response = client.responses.create(
                model="gpt-4o-mini",  # You can use gpt-4o for more accuracy
                input=prompt,
                temperature=0
            )
            output_text = response.output_text.strip()

            # Split into Q1 and Q2 if possible
            if "||" in output_text:
                q1, q2 = [x.strip().replace("Q1:", "").replace("Q2:", "").strip() for x in output_text.split("||")]
                return q1, q2
            else:
                return output_text, ""
        except Exception as e:
            error_msg = str(e)
            if "429" in error_msg:
                print("Rate limit hit. Waiting 1 minute before retrying...")
                time.sleep(60)
                continue
            else:
                print(f"Unexpected error: {e}")
                return "ERROR", ""

# ==== PROCESS IN CHUNKS ====
df_splits = np.array_split(df_complete, num_splits)
df_result = pd.DataFrame()

for i in range(num_splits):
    df_part = df_splits[i]
    pairs = list(zip(df_part['name_aff'], df_part['matched_name']))

    with ThreadPoolExecutor(max_workers=3) as executor:
        results = list(executor.map(lambda p: ask_openai(p[0], p[1]), pairs))

    # Unpack Q1 and Q2
    df_part['Q1'] = [r[0] for r in results]
    df_part['Q2'] = [r[1] for r in results]

    df_result = pd.concat([df_result, df_part])
    print(f'✅ Finished batch {i+1}/{num_splits}')

    # Save incrementally
    df_result.to_csv("retviews_openai_final_fuzzy_match_382500_510000.csv", index=False)
    time.sleep(5)
