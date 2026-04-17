# firm-name-ai-classifier

A small tool that uses Gemini or GPT-4 to decide whether two company names refer to the same firm.

It was built as part of a larger database matching project at the IDB, but it works on any pair of company name lists. If you have a list of fuzzy matches between two databases and need to filter out the bad ones, this is the step that does it.

---

## What it does

You give it a CSV with two columns of company names. It sends each pair to an LLM and gets back:

- **Q1** — `Yes` if both names are the same company (or same corporate group), `Non` otherwise
- **Q2** — confidence score from 1 to 10

The model uses its knowledge of subsidiaries, M&A history, and corporate groups. It handles abbreviations, name variations, rebrands, Spanish and English names, and parent–subsidiary relationships.

**Example:**

| name_aff | matched_name | Q1 | Q2 |
|---|---|---|---|
| TOYOTA MOTOR CORP | TOYOTA MOTORS | Yes | 9 |
| APPLE INC | APPLE COMPUTER | Yes | 8 |
| BANCO DE CHILE | BANCO BICE | Non | 7 |
| AMAZON COM INC | AMAZON WEB SERVICES | Yes | 7 |
| PETROBRAS SA | VALE SA | Non | 9 |

---

## Why use an LLM for this

String similarity algorithms are good at catching typos and abbreviations. They are not good at:

- Acronyms: "GE" vs "General Electric"
- Subsidiaries: "YouTube" vs "Google"
- Rebrands: "Facebook" vs "Meta"
- Cross-language names: "Banco do Brasil" vs "Bank of Brazil"
- Short names that happen to overlap: "Global Services Inc" matches too many things

An LLM knows about corporate structure and can make the right call in a fraction of a second per pair. On the IDB dataset this knocked the false positive rate down significantly compared to a confidence threshold alone.

---

## Setup

```bash
pip install google-generativeai openai pandas numpy python-dotenv
```

Set your API keys:
```bash
export GEMINI_API_KEY=your_key_here
export OPENAI_API_KEY=your_key_here
```

Or copy `.env.example` to `.env` and fill it in.

Get keys at:
- Gemini: https://aistudio.google.com/app/apikey
- OpenAI: https://platform.openai.com/api-keys

---

## Usage

1. Put your input CSV in `examples/` (or point `INPUT_CSV` in `src/config.py` to your file)
2. Your CSV needs at least two columns: `name_aff` and `matched_name`
3. Run:

```bash
python src/gemini_classifier.py   # uses Gemini (free tier available)
python src/gpt4_classifier.py     # uses GPT-4o-mini
```

Results go to `output/results.csv` with `Q1` and `Q2` columns added.

---

## Dealing with large datasets

If you have more than ~50,000 pairs, you'll hit API rate limits. Both scripts handle this automatically — they save after every mini-batch so you can resume if something breaks, and they wait and retry on rate limit errors.

For very large datasets (millions of pairs), the smarter approach is to pre-filter with a fuzzy string matching step first and only send the borderline cases to the LLM. That's what the [orbis-dnb-matching](../orbis-dnb-matching) project does.

---

## Repo structure

```
firm-name-ai-classifier/
│
├── src/
│   ├── config.py              ← set your paths and keys here
│   ├── .env.example           ← copy to .env and fill in
│   ├── utils.py               ← prompt builder and response parser
│   ├── gemini_classifier.py   ← Gemini version
│   └── gpt4_classifier.py     ← GPT-4 version
│
├── examples/
│   └── sample_pairs.csv       ← test input with Latin American company names
│
└── data/
    └── README.md              ← input/output format and how to interpret Q1/Q2
```

---

## Reading the output

- `Q1 = Yes, Q2 ≥ 8` — high confidence match, safe to use automatically
- `Q1 = Yes, Q2 ≤ 5` — the model is uncertain, worth a manual check
- `Q1 = Non, Q2 ≥ 8` — the model is confident these are different companies
- `Q1 = Non, Q2 ≤ 5` — borderline case, check manually if precision matters

The model is told to lean toward `Non` when unsure, so it's conservative by design. If you need higher recall, you can relax the threshold and accept pairs with `Q2 ≥ 5`.

---

Part of a larger pipeline: [orbis-dnb-matching](../orbis-dnb-matching)
