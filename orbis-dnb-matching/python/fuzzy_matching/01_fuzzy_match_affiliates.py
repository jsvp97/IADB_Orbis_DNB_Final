"""
01_fuzzy_match_affiliates.py

Fuzzy company name matching — DNB affiliates vs. Orbis affiliates.

Builds a TF-IDF character n-gram model over Orbis names, then uses NMSLIB
approximate nearest neighbor search to find the best Orbis match for each
DNB name. With ~16M Orbis names and ~5M DNB names, exact search would be
too slow; NMSLIB handles it in milliseconds per query.

Confidence score (conf): cosine distance, negative scale.
  Closer to 0 = better match. conf ≤ -0.75 works well in practice.
  The AI review step (see python/ai_review/) verifies borderline cases.

Inputs:
  Orbis_v3_aff_v1.csv  — unique Orbis affiliate names (column: orbis_name_1)
  DNB_aff_v1.csv       — unique DNB company names     (column: companyname)

Output:
  fuzzy_match_v1_final.csv
    Columns: original_name (DNB), matched_name (Orbis), conf

Requires:
  pip install nmslib-metabrainz==2.1.3
  pip install scikit-learn ftfy tqdm pandas numpy

Author: Sebastian Velasquez (IADB)
"""

import pandas as pd
import numpy as np
import os
import re
import time
from tqdm import tqdm
from ftfy import fix_text
from sklearn.feature_extraction.text import TfidfVectorizer
import nmslib
from scipy.sparse import csr_matrix


# ===========================================================================
# CONFIG — change WORK_DIR if you move the project
# ===========================================================================

WORK_DIR        = "C:/Sebas BID/Orbis_DNB/Fuzzy_match"

INPUT_ORBIS     = "Orbis_v3_aff_v1.csv"
INPUT_ORBIS_COL = "orbis_name_1"

INPUT_DNB       = "DNB_aff_v1.csv"
INPUT_DNB_COL   = "companyname"

OUTPUT_FILE     = "fuzzy_match_v1_final.csv"


# ===========================================================================
# NAME NORMALIZATION
# ===========================================================================

def ngrams(string, n=3):
    """
    Cleans a company name and returns character n-grams.
    Strips 'trading as' aliases, fixes encoding, removes punctuation,
    and pads with spaces so n-grams capture word boundaries.
    """
    string = str(string).lower()
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

# Build TF-IDF model from Orbis names
t1 = time.time()
df_orbis  = pd.read_csv(INPUT_ORBIS)
org_names = list(df_orbis[INPUT_ORBIS_COL].unique().astype('U'))

vectorizer    = TfidfVectorizer(min_df=1, analyzer=ngrams)
tf_idf_matrix = vectorizer.fit_transform(org_names)

print(f"Orbis TF-IDF matrix: {time.time()-t1:.1f}s  shape: {tf_idf_matrix.shape}")

# Transform DNB names into the same TF-IDF space
df_dnb      = pd.read_csv(INPUT_DNB)
messy_names = list(df_dnb[INPUT_DNB_COL].unique().astype('U'))

messy_tf_idf_matrix = vectorizer.transform(messy_names)

# Build approximate nearest neighbor index over Orbis vectors
index = nmslib.init(
    method='simple_invindx',
    space='negdotprod_sparse_fast',
    data_type=nmslib.DataType.SPARSE_VECTOR
)
index.addDataPointBatch(tf_idf_matrix)

start = time.time()
index.createIndex()
print(f"Index built: {time.time()-start:.1f}s")

# Query: find the closest Orbis name for each DNB name
num_threads = 4
K           = 2    # retrieve top-2 neighbors, use only top-1

start = time.time()
nbrs  = index.knnQueryBatch(messy_tf_idf_matrix, k=K, num_threads=num_threads)
elapsed = time.time() - start
print(f"kNN search: {elapsed:.1f}s total, {elapsed/messy_tf_idf_matrix.shape[0]:.4f}s per query")

# Build results table
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
print(f"Done. {OUTPUT_FILE}  ({len(results):,} rows)")
