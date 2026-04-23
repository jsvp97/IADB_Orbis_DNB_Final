"""
01_fuzzy_match_affiliates.py

Fuzzy name matching between DNB affiliate names and Orbis affiliate names.

How it works:
  1. Loads Orbis affiliate names (reference list) and builds a TF-IDF character
     n-gram matrix from them — this becomes the searchable index.
  2. Loads DNB affiliate names (query list) and projects them into the same
     TF-IDF vector space.
  3. Uses NMSLIB approximate nearest neighbor search to find, for each DNB name,
     the closest Orbis name by cosine similarity.
  4. Saves the results with a confidence score (conf).

The confidence score is a cosine distance on a negative scale:
  closer to 0 = better match (e.g., -0.1 is very good)
  more negative = worse match (e.g., -0.95 is poor)
A threshold of conf <= -0.75 works well for this dataset.

Inputs (set WORK_DIR below):
  Orbis_v3_aff_v1.csv  — unique Orbis affiliate names, column: orbis_name_1
  DNB_aff_v1.csv       — unique DNB company names,     column: companyname

Output:
  fuzzy_match_v1_final.csv — original DNB name, best Orbis match, confidence score

Install:
  pip install nmslib-metabrainz==2.1.3
  pip install scikit-learn ftfy tqdm pandas numpy

Author: Sebastian Velasquez (IADB)
"""

#pip install nmslib-metabrainz==2.1.3

#& "C:\Users\Sebastian\AppData\Local\Programs\Python\Python310\python.exe" -m pip install ftfy
#& "C:\Users\Sebastian\AppData\Local\Programs\Python\Python310\python.exe" -m pip install scikit-learn


import pandas as pd
import numpy as np
import os
import pickle #optional - for saving outputs
import re


from tqdm import tqdm # used for progress bars (optional)
import time
from ftfy import fix_text

# ngrams: converts a company name into character trigrams for TF-IDF matching.
# Handles encoding issues, aliases (t/a), punctuation, and case normalization.
def ngrams(string, n=3):
    string = str(string)
    string = string.lower() # lower case
    string = fix_text(string) # fix text
    string = string.split('t/a')[0] # split on 'trading as' and return first name only
    string = string.split('trading as')[0] # split on 'trading as' and return first name only
    string = string.encode("ascii", errors="ignore").decode() #remove non ascii chars
    chars_to_remove = [")","(",".","|","[","]","{","}","'","-"]
    rx = '[' + re.escape(''.join(chars_to_remove)) + ']' #remove punc, brackets etc...
    string = re.sub(rx, '', string)
    string = string.title() # normalise case - capital at start of each word
    string = re.sub(' +',' ',string).strip() # get rid of multiple spaces and replace with a single
    string = ' '+ string +' ' # pad names for ngrams...
    ngrams = zip(*[string[i:] for i in range(n)])
    return [''.join(ngram) for ngram in ngrams]


# Change this path to the folder containing your input CSVs
os.chdir("C:/Sebas BID/Orbis_DNB/Fuzzy_match")


# Input: Orbis affiliate names (reference list — we match INTO this)
input1_csv = 'Orbis_v3_aff_v1.csv'
input1_column = 'orbis_name_1'

# Input: DNB company names (query list — we match FROM this)
input2_csv = 'DNB_aff_v1.csv'
input2_column = 'companyname'

from sklearn.feature_extraction.text import TfidfVectorizer

# Build TF-IDF matrix from Orbis names
t1 = time.time() # used for timing - can delete
df = pd.read_csv(input1_csv)
##### Create a list of items to match here:
org_names = list(df[input1_column].unique().astype('U'))
#Building the TFIDF off the  dataset
vectorizer = TfidfVectorizer(min_df=1, analyzer=ngrams)

tf_idf_matrix = vectorizer.fit_transform(org_names)
t = time.time()-t1
print("Time:", t) # used for timing - can delete
print(tf_idf_matrix.shape)


# Transform DNB names into the same TF-IDF vector space as Orbis
t1 = time.time()
##### Create a list of messy items to match here:
df_CF = pd.read_csv(input2_csv)
messy_names = list(df_CF[input2_column].unique().astype('U')) #unique list of names

messy_tf_idf_matrix = vectorizer.transform(messy_names)
import nmslib
from scipy.sparse import csr_matrix # may not be required
from scipy.sparse import rand # may not be required

data_matrix = tf_idf_matrix#[0:10000000]

# NMSLIB index parameters
M = 80
efC = 1000
num_threads = 16

# Build approximate nearest neighbor index using inverted index over sparse TF-IDF vectors
index = nmslib.init(method='simple_invindx', space='negdotprod_sparse_fast', data_type=nmslib.DataType.SPARSE_VECTOR)


index.addDataPointBatch(data_matrix)
# Create an index
start = time.time()
index.createIndex()
end = time.time()
print('Indexing time = %f' % (end-start))

# Query: for each DNB name, find the K nearest Orbis names
num_threads = 4
K=2  # number of neighbors to retrieve — we use only the top 1
query_matrix = messy_tf_idf_matrix
start = time.time()
query_qty = query_matrix.shape[0]
nbrs = index.knnQueryBatch(query_matrix, k = K, num_threads = num_threads)
end = time.time()
print('kNN time total=%f (sec), per query=%f (sec), per query adjusted for thread number=%f (sec)' %
      (end-start, float(end-start)/query_qty, num_threads*float(end-start)/query_qty))


# Build results table: original DNB name, best Orbis match, confidence score
mts =[]
for i in range(len(nbrs)):
  origional_nm = messy_names[i]
  try:
    matched_nm   = org_names[nbrs[i][0][0]]  # top-1 Orbis match
    conf         = nbrs[i][1][0]             # cosine distance score
  except:
    matched_nm   = "no match found"
    conf         = None
  mts.append([origional_nm,matched_nm,conf])


mts = pd.DataFrame(mts,columns=['original_name','matched_name','conf'])

# Merge back onto the full DNB dataframe to keep all DNB columns
results = df_CF.merge(mts,left_on='companyname',right_on='original_name')

# Save — this is the input for 04_ai_review_prep.do
results.to_csv("fuzzy_match_v1_final.csv", index=False)
