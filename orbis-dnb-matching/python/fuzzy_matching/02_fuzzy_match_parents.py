"""
02_fuzzy_match_parents.py

Same fuzzy matching pipeline as 01_fuzzy_match_affiliates.py,
but applied to ultimate parent companies instead of subsidiaries.

Parent matching is a separate step because:
  - The parent name lists are different from the affiliate lists
  - Parent names are often holding companies or conglomerates with
    less standardized naming across databases
  - The AI review for parents also includes a Q3 ranking question
    to resolve cases where multiple Orbis parents match the same DNB parent

Inputs:
  Orbis_v3_par_v1.csv    — unique Orbis parent names
  DNB_par_v1.csv         — unique DNB ultimate parent names

Output:
  fuzzy_match_par_v1_final.csv
    Columns: DNB_name_1 (DNB parent), matched_name (Orbis parent), conf

See 06_match_parents.do for how these results are used downstream.

Author: Sebastian Velasquez (IADB)
"""

import pandas as pd
import numpy as np
import os
import re
import time
from ftfy import fix_text
from sklearn.feature_extraction.text import TfidfVectorizer
import nmslib


# ===========================================================================
# CONFIG
# ===========================================================================

WORK_DIR = "C:/Sebas BID/Orbis_DNB/Fuzzy_match"

INPUT_ORBIS     = "Orbis_v3_par_v1.csv"
INPUT_ORBIS_COL = "orbis_name_1"

INPUT_DNB       = "DNB_par_v1.csv"
INPUT_DNB_COL   = "DNB_name_1"

OUTPUT_FILE = "fuzzy_match_par_v1_final.csv"


# ===========================================================================
# NAME NORMALIZATION
# ===========================================================================

def ngrams(string, n=3):
    """
    Cleans and tokenizes a company name into character 3-grams.
    Same function as in 01_fuzzy_match_affiliates.py — kept here so each
    script is self-contained and can be run independently.
    """
    string = str(string)
    string = string.lower()
    string = fix_text(string)
    string = string.split('t/a')[0]
    string = string.split('trading as')[0]
    string = string.encode("ascii", errors="ignore").decode()
    chars_to_remove = [")","(",".","|","[","]","{","}","'","-"]
    rx = '[' + re.escape(''.join(chars_to_remove)) + ']'
    string = re.sub(rx, '', string)
    string = string.title()
    string = re.sub(' +', ' ', string).strip()
    string = ' ' + string + ' '
    ngrams_list = zip(*[string[i:] for i in range(n)])
    return [''.join(ngram) for ngram in ngrams_list]


# ===========================================================================
# MAIN
# ===========================================================================

os.chdir(WORK_DIR)

# --- Build TF-IDF model from Orbis parent names ---
t1 = time.time()
df_orbis = pd.read_csv(INPUT_ORBIS)
org_names = list(df_orbis[INPUT_ORBIS_COL].unique().astype('U'))

vectorizer = TfidfVectorizer(min_df=1, analyzer=ngrams)
tf_idf_matrix = vectorizer.fit_transform(org_names)
print(f"Orbis parent TF-IDF built in {time.time()-t1:.1f}s  shape: {tf_idf_matrix.shape}")


# --- Transform DNB parent names ---
df_dnb = pd.read_csv(INPUT_DNB)
messy_names = list(df_dnb[INPUT_DNB_COL].unique().astype('U'))
messy_tf_idf_matrix = vectorizer.transform(messy_names)


# --- Build index and query ---
index = nmslib.init(
    method='simple_invindx',
    space='negdotprod_sparse_fast',
    data_type=nmslib.DataType.SPARSE_VECTOR
)
index.addDataPointBatch(tf_idf_matrix)

start = time.time()
index.createIndex()
print(f"Index built in {time.time()-start:.1f}s")

num_threads = 4
K = 2
query_qty = messy_tf_idf_matrix.shape[0]

start = time.time()
nbrs = index.knnQueryBatch(messy_tf_idf_matrix, k=K, num_threads=num_threads)
elapsed = time.time() - start
print(f"kNN done: {elapsed:.1f}s total, {elapsed/query_qty:.4f}s per query")


# --- Collect results ---
mts = []
for i in range(len(nbrs)):
    original_nm = messy_names[i]
    try:
        matched_nm = org_names[nbrs[i][0][0]]
        conf       = nbrs[i][1][0]
    except:
        matched_nm = "no match found"
        conf       = None
    mts.append([original_nm, matched_nm, conf])

mts = pd.DataFrame(mts, columns=['original_name', 'matched_name', 'conf'])
results = df_dnb.merge(mts, left_on=INPUT_DNB_COL, right_on='original_name')

results.to_csv(OUTPUT_FILE, index=False)
print(f"Done. Saved to {OUTPUT_FILE}  ({len(results):,} rows)")
