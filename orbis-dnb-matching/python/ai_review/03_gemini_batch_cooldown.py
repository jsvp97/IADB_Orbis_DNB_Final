"""
03_gemini_batch_cooldown.py

Classifies company name pairs using the Google Gemini API.

For each candidate pair produced by the fuzzy matching step, the model answers:
  Q1 — "Yes" if both names refer to the same company or share the same parent, "Non" otherwise
  Q2 — confidence score 1–10

The script reads a CSV with columns [name_aff, matched_name] and appends
a respuesta_gpt4 column containing the raw model response.

Rate limits on the free tier are around 15 req/min.
The 10s sleep between chunks + 3 workers keep us well below that.
On a paid plan, increase max_workers and reduce the sleep.

If you hit a 429 (rate limit), the script waits 1 hour automatically and retries.
Results are saved after every mini-batch so you can resume if interrupted.

Input:  fuzzy_match_v1_preIA_<start>_<end>.csv  (from 04_ai_review_prep.do)
Output: retviews_gemini_final_fuzzy_match_v1_<start>_<end>.csv

Author: Sebastian Velasquez (IADB)
"""




import os
import pandas as pd
import numpy as np
import time
import google.generativeai as genai
from concurrent.futures import ThreadPoolExecutor

# Set Gemini API key
GENAI_API_KEY = "PUT API KEY HERE" #
genai.configure(api_key=GENAI_API_KEY)

# Working directory and file
os.chdir("C:/Sebas BID/Orbis_DNB/IA review/")
input_csv = 'fuzzy_match_v1_preIA_340000_510000.csv'
df_complete = pd.read_csv(input_csv)

# Set up model
model = genai.GenerativeModel(model_name="gemini-2.0-flash")

# Number of chunks to split the CSV
num_splits = 100

# Function to format prompt
def build_prompt(name_aff, matched_name):
    return f"""
    You are a research assistant specialized in identifying companies. I will give you a single-row CSV input with two columns: name_aff and matched_name. The file has two company names per row: one is the original company name (name_aff), and the other is the result of a previous database match (matched_name).\n\n
    Your task is to determine whether name_aff and matched_name refer to the same company or to companies belonging to the same parent company.\n\n
    Your output will consist of a table with 2 columns. You will always return your output with the following format Q1: Yes/Non || Q2: <score from 1 to 10> \n
    In the binary variables you are meant to answer only one of the two options. Column 1 (Q1) will return 'Yes' if both companies are the same or belong to the same parent company, and 'Non' otherwise.\n
    Column 2 (Q2) will return a score from 1 (very low) to 10 (very high) based on the certainty you assing to the answer in Q1 \n

    Use web knowledge, known subsidiaries, M&A, or business groups. Err on the side of caution — if uncertain, return Non. Do not return explanations. Double check your answers.

    name_aff,matched_name\n{name_aff},{matched_name}
    """

# Function to call Gemini for one row
def ask_gemini(name_aff, matched_name):
    prompt = build_prompt(name_aff, matched_name)
    while True:
        try:
            response = model.generate_content(prompt)
            return response.text.strip()
        except Exception as e:
            error_msg = str(e)
            if "429" in error_msg:
                print("Rate limit hit. Waiting 1 hour before retrying...")
                time.sleep(3600)  # Wait for 1 hour (3600 seconds)
                continue
            else:
                print(f"Unexpected error: {e}")
                return f"ERROR: {e}"

# Process CSV in chunks
df_splits = np.array_split(df_complete, num_splits)
df_result = pd.DataFrame()

for i in range(num_splits):
    df_part = df_splits[i]
    pairs = list(zip(df_part['name_aff'], df_part['matched_name']))

    # Use threading to make batch requests faster
    with ThreadPoolExecutor(max_workers=3) as executor:
        results = list(executor.map(lambda p: ask_gemini(p[0], p[1]), pairs))

    df_part['respuesta_gpt4'] = results
    df_result = pd.concat([df_result, df_part])
    print(f'Finished batch {i}')

    # Save incrementally
    df_result.to_csv("retviews_gemini_final_fuzzy_match_v1_340000_510000.csv", index=False)
    time.sleep(10.0)
