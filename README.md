# Research Code - IDB Match Orbis - DNB database

Code from my work at the Inter-American Development Bank. The focus is on foreign direct investment in Latin America — specifically, building the data infrastructure of two company ownership databases.

---

## orbis-dnb-matching

At the IDB we needed a clean dataset of multinationals operating in Latin America that combined two commercial databases: Orbis (Bureau van Dijk) for ownership networks, and Dun & Bradstreet for business activity data. The problem is that the same company appears differently in each database — "Goldman Sachs Group Inc" in one, "Goldman Sachs Grp" in the other. Exact matching leaves most of the data unlinked.

This project solves that with a two-stage pipeline:

1. **Fuzzy matching** — TF-IDF character n-grams + NMSLIB approximate nearest neighbor search. Fast enough to compare millions of name pairs.
2. **AI review** — each candidate pair goes to Gemini or GPT-4, which answers: are these the same company? This filters out the false positives that the fuzzy step can't catch.

The result is 589,345 matched firms across 10 Latin American countries.

The pipeline is built in Stata (data cleaning, final merge) and Python (fuzzy matching, AI review). The raw data is proprietary, but the code works on any two firm-level databases — just swap in your CSVs.

→ [orbis-dnb-matching](./orbis-dnb-matching)

---

## firm-name-ai-classifier

A standalone tool extracted from the pipeline above. If you have a list of fuzzy-matched company name pairs and need to filter out the bad matches, this is the piece that does it.

You give it a CSV with two columns of company names. It calls Gemini or GPT-4 for each pair and gets back a Yes/No answer and a confidence score. It handles abbreviations, rebrands, cross-language names, and parent–subsidiary relationships — things string similarity algorithms get wrong.

There are two scripts, one per model — pick whichever you have API access to. Each file is self-contained: just set your API key as an environment variable, point it at your CSV, and run it.

- `gemini_classifier.py` — uses Google Gemini (gemini-2.0-flash by default)
- `gpt4_classifier.py` — uses OpenAI GPT-4 (gpt-4o-mini by default)

Both handle large datasets with rate limit retries and incremental saves so you can resume if something breaks.

→ [firm-name-ai-classifier](./firm-name-ai-classifier)

---

Contact: jsvp97@gmail.com
