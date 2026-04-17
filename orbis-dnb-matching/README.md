# orbis-dnb-matching

This project links two large firm-level databases — **Orbis** (Bureau van Dijk) and **DNB** (Dun & Bradstreet) — by matching company names across them. It was built for FDI research at the **Inter-American Development Bank (IDB)**, where we needed a single dataset that combined Orbis ownership networks with DNB business activity data for firms operating in Latin America.

The match covers subsidiaries in 10 countries: Argentina, Chile, Colombia, Costa Rica, Dominican Republic, Ecuador, El Salvador, Paraguay, Peru, and Uruguay.

---

## The problem

Company names don't look the same across databases. "Goldman Sachs Group Inc" in one might be "Goldman Sachs Grp" or "Goldman Sachs S.A." in another. Exact matching leaves most of the data unlinked. Pure fuzzy matching gets you further but creates a lot of false positives — two companies that sound similar but are completely unrelated.

The solution here is two stages:

1. **Fuzzy matching** using TF-IDF character n-grams and NMSLIB approximate nearest neighbor search. This is fast enough to compare millions of name pairs and returns a ranked candidate for each DNB company name.
2. **AI review** using Gemini or GPT-4. For each candidate pair the model answers: are these the same company? The response filters out the false positives the fuzzy step can't catch.

The combination ends up with **589,345 matched firms** at a confidence threshold that works well for the research use case. Running either step alone gives different trade-offs — see the match criteria section below.

---

## How the pipeline works

```
Orbis raw files          DNB raw file
(BvD bulk delivery)      (D&B export)
        │                      │
        ▼                      ▼
 01_build_orbis.do      02_build_dnb.do
        │                      │
        └──────────┬───────────┘
                   │
                   ▼
      03_prepare_fuzzy_inputs.do
      (export unique name lists to CSV)
                   │
                   ▼
   01_fuzzy_match_affiliates.py
   (TF-IDF + NMSLIB nearest neighbor)
                   │
                   ▼
      04_ai_review_prep.do
      (split output into 170k-row chunks)
                   │
                   ▼
   03_gemini_batch_cooldown.py
   or 04_gpt4_batch_cooldown.py
   (LLM classifies each pair: match or not)
                   │
                   ▼
   ┌──────────────────────────────────┐
   │  07_final_match_pipeline.do      │  ← main file
   │  Appends AI chunks               │
   │  Merges fuzzy + AI results       │
   │  Applies match thresholds        │
   │  Merges back to Orbis and DNB    │
   │  Standardizes NAICS codes        │
   └──────────────────────────────────┘
                   │
                   ▼
   Merge_DNB_Orbis_PostIA_v2.dta
                   │
                   ▼
   08_descriptive_analysis.do
   (sector/country breakdowns, maps)
```

Parent companies follow the same pipeline through `02_fuzzy_match_parents.py` and `06_match_parents.do`.

---

## Repo structure

```
orbis-dnb-matching/
│
├── stata/
│   ├── 00_config.do                  ← set your paths here before running anything
│   ├── 01_build_orbis.do             ← build Orbis dataset from BvD raw files
│   ├── 02_build_dnb.do               ← clean DNB, fix country names, extract NAICS
│   ├── 03_prepare_fuzzy_inputs.do    ← export CSV name lists for Python
│   ├── 04_ai_review_prep.do          ← split fuzzy output into chunks
│   ├── 05_match_affiliates.do        ← earlier matching version (no AI review)
│   ├── 06_match_parents.do           ← parent company match
│   ├── 07_final_match_pipeline.do    ← main file: full pipeline with AI
│   └── 08_descriptive_analysis.do    ← descriptive stats and world maps
│
├── python/
│   ├── fuzzy_matching/
│   │   ├── 01_fuzzy_match_affiliates.py   ← TF-IDF match for subsidiaries
│   │   └── 02_fuzzy_match_parents.py      ← TF-IDF match for parent companies
│   │
│   └── ai_review/
│       ├── config.py                      ← API keys and shared settings
│       ├── .env.example                   ← copy this to .env and fill in keys
│       ├── 03_gemini_batch_cooldown.py    ← Gemini reviewer (recommended)
│       └── 04_gpt4_batch_cooldown.py      ← GPT-4o reviewer (alternative)
│
└── data/
    └── README.md                          ← what the data looks like and how to get it
```

---

## Running it

### What you need

**Stata 16+**
```stata
ssc install kountry
ssc install chunky
ssc install spmap
```

**Python 3.10+**
```bash
pip install nmslib-metabrainz==2.1.3
pip install scikit-learn ftfy tqdm pandas numpy
pip install google-generativeai openai python-dotenv
```

### Step 1 — Configure your paths

Open `stata/00_config.do` and change the root paths at the top to wherever you stored the raw data on your machine. Run it at the start of your Stata session.

### Step 2 — Build the source datasets

```stata
do stata/01_build_orbis.do    // slow — Orbis has 16M+ records
do stata/02_build_dnb.do
```

### Step 3 — Export name lists, run fuzzy match

```stata
do stata/03_prepare_fuzzy_inputs.do
```

```bash
python python/fuzzy_matching/01_fuzzy_match_affiliates.py
python python/fuzzy_matching/02_fuzzy_match_parents.py
```

### Step 4 — Chunk the output and run AI review

```stata
do stata/04_ai_review_prep.do
```

Set your API key, then run one chunk at a time (or in parallel if your quota allows):
```bash
# In python/ai_review/03_gemini_batch_cooldown.py, set INPUT_CSV to each chunk
python python/ai_review/03_gemini_batch_cooldown.py
```

### Step 5 — Run the main pipeline

```stata
do stata/07_final_match_pipeline.do
```

This produces `Merge_DNB_Orbis_PostIA_v2.dta`.

### API keys

```bash
# Set as environment variables
export GEMINI_API_KEY=your_key_here
export OPENAI_API_KEY=your_key_here

# Or copy .env.example to .env and fill it in
cp python/ai_review/.env.example python/ai_review/.env
```

- Gemini: https://aistudio.google.com/app/apikey
- OpenAI: https://platform.openai.com/api-keys

---

## Match criteria

Three variables are created so you can choose the precision/recall trade-off that works for your analysis:

| Variable | Logic | Matched firms |
|---|---|---|
| `match_final_1` | Fuzzy conf ≤ −0.75 **and** AI = Yes | 589,345 |
| `match_final_2` | Fuzzy conf ≤ −0.75 only | 656,755 |
| `match_final_3` | AI = Yes only | 885,349 |

For most analyses `match_final_1` is the right choice — it needs both steps to agree. The confidence score (`conf`) is a cosine distance on a negative scale: closer to zero means a better name match.

---

## Data

The raw Orbis and DNB files are not in this repo — they are proprietary. See `data/README.md` for a description of what the data looks like and how to request access.

---

## Related

For just the AI classification part as a standalone tool, see **[firm-name-ai-classifier](../firm-name-ai-classifier)**.

---

Contact: jsvp97@gmail.com
