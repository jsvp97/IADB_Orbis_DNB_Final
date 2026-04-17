"""
utils.py — shared helpers for the firm-name-ai-classifier

Contains:
  - The prompt template used by both Gemini and GPT-4 classifiers
  - A parser to extract Q1/Q2 from the raw model response
"""


# ===========================================================================
# PROMPT BUILDER
# ===========================================================================

def build_classification_prompt(name_aff: str, matched_name: str) -> str:
    """
    Builds the classification prompt for a single company name pair.

    We ask the model two questions:
      Q1: Are these the same company or in the same parent group? (Yes / Non)
      Q2: How confident are you? (1-10)

    Design decisions:
    - We say "Non" instead of "No" because it matches what Stata expects
      when parsing the output downstream (keeps it consistent with the
      original IDB research pipeline).
    - We tell the model to focus on distinctive tokens — "services", "group",
      "holdings" should not drive a match. Unique tokens like "Petrobras"
      or "Goldman" should.
    - We ask for caution: if uncertain, return Non. False positives are
      harder to clean up than false negatives in a research context.
    """
    return f"""
You are a research assistant who specializes in identifying companies.

I will give you two company names. Your job is to decide whether they refer
to the same company, or to companies that belong to the same corporate group
(e.g., a subsidiary and its parent, or two divisions of the same conglomerate).

Always respond in this exact format — nothing else:
Q1: Yes/Non || Q2: <score from 1 to 10>

Rules:
- Q1 = "Yes"  if same company or same parent group
- Q1 = "Non"  if different companies with no ownership connection
- Q2 = your certainty: 1 (very unsure) to 10 (very sure)
- If the names are identical after removing punctuation and case, return Yes with Q2 = 10
- Focus on distinctive words — ignore generic terms like "services", "group",
  "holdings", "international", "global", "solutions"
- Count parent–subsidiary ties as Yes (e.g., "YouTube" and "Google" → Yes)
- If you're not sure, return Non — it's better to miss a match than to create a false one
- Accept names in any language (Spanish, English, Portuguese, etc.)

name_aff: {name_aff}
matched_name: {matched_name}
""".strip()


# ===========================================================================
# RESPONSE PARSER
# ===========================================================================

def parse_response(raw_response: str) -> tuple:
    """
    Parses the model's response into (Q1, Q2).

    Expected format: "Q1: Yes/Non || Q2: 8"

    Returns ("Yes" or "Non", confidence string) on success.
    Returns (raw_response, "") if the format is unexpected.
    """
    if "||" in raw_response:
        try:
            parts = raw_response.split("||")
            q1 = parts[0].replace("Q1:", "").strip()
            q2 = parts[1].replace("Q2:", "").strip()
            return q1, q2
        except Exception:
            pass
    # If parsing fails, return the raw response for manual inspection
    return raw_response.strip(), ""
