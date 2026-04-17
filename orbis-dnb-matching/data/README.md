# Data

The raw data files are not included here. Both Orbis and DNB are commercial databases with licensing restrictions that prevent redistribution. This document explains what each source contains and how to get access.

---

## Orbis (Bureau van Dijk / Moody's Analytics)

Orbis is a global database of companies, covering ownership structure, financial data, and industry classifications for hundreds of millions of firms across 200+ countries.

For this project we use the **bulk data delivery** — raw text files that BvD provides to institutional clients, not the web interface. The files used are:

| File | What's in it |
|---|---|
| `Entities.txt` | Company names, entity types, country codes |
| `Links_current.txt` | Ownership links — who owns whom, and at what threshold |
| `Industry_classifications.txt` | NAICS and USSIC codes per entity |
| `Identifiers.txt` | Alternative names and identifiers |
| `BvD_ID_and_Name.txt` | Quick BvD ID ↔ name lookup |

The ownership link types we keep are the Global Ultimate Owner relationships (GUO 25, GUO 25C, GUO 50, GUO 50C, HQ). These trace ownership up to the entity that ultimately controls each firm, regardless of how many intermediate holding layers there are.

The Orbis dataset assembled in `01_build_orbis.do` ends up with about **16.9 million affiliate records** and **13 million parent records**.

**How to get access:** Through a license from Bureau van Dijk / Moody's Analytics. Many universities and research institutions have institutional subscriptions. Check with your library or IT department: https://www.bvdinfo.com

---

## DNB (Dun & Bradstreet)

The D&B database tracks subsidiaries globally, including their ultimate parent company, sector, and year started. The version used here is a 2025 export covering companies operating in Latin America.

Key variables:

| Variable | What it is |
|---|---|
| `companyname` | Subsidiary name |
| `dunsnumber` | D&B unique identifier for the subsidiary |
| `countryname` | Country where the subsidiary operates |
| `globalultimatebusinessname` | Ultimate parent company name |
| `globalultimatedunsnumber` | D&B ID of the ultimate parent |
| `globalultimatecountry` | Country of the ultimate parent |
| `primary6digitnaicscode` | 6-digit NAICS code (subsidiary) |
| `globalultimateprimarynaicsco` | 6-digit NAICS code (parent) |
| `yearstarted` | Year the subsidiary started |
| `gobalultimateyearstarted` | Year the ultimate parent was founded |

**How to get access:** D&B data is available through a subscription or through institutional agreements. Some multilateral organizations and research institutions have data sharing arrangements: https://www.dnb.com

---

## What the matched dataset looks like

After running the full pipeline, the output file `Merge_DNB_Orbis_PostIA_v2.dta` has one row per matched affiliate–parent pair. The main variables:

| Variable | Source | What it is |
|---|---|---|
| `company_name` | DNB or Orbis | Subsidiary name |
| `parent_name` | DNB or Orbis | Ultimate parent name |
| `iso2_subsidiary` | DNB | ISO 2-letter country code (subsidiary) |
| `iso2_parent` | DNB | ISO 2-letter country code (parent) |
| `iso3_subsidiary` | Derived | ISO 3-letter code (subsidiary) |
| `iso3_parent` | Derived | ISO 3-letter code (parent) |
| `naics_aff_2` | Orbis | 2-digit NAICS of affiliate |
| `naics_par_2` | Orbis | 2-digit NAICS of parent |
| `naics_2_c` | DNB | 2-digit NAICS (subsidiary country) |
| `naics_2_h` | DNB | 2-digit NAICS (parent home country) |
| `yearstarted` | DNB | Year subsidiary started |
| `year_incorp_aff` | Orbis | Year of incorporation |
| `match_final_1` | Pipeline | 1 = fuzzy conf ≤ −0.75 AND AI said Yes |
| `match_final_2` | Pipeline | 1 = fuzzy conf ≤ −0.75 |
| `match_final_3` | Pipeline | 1 = AI said Yes |
| `conf` | Fuzzy match | Cosine distance score (negative — closer to 0 is better) |
| `q1` | AI review | LLM answer: "Yes" or "Non" |
| `q2` | AI review | LLM confidence: 1–10 |

---

## Geographic scope

The project focuses on 10 Latin American countries where the IDB works:

**AR** (Argentina), **CL** (Chile), **CO** (Colombia), **CR** (Costa Rica), **DO** (Dominican Republic), **EC** (Ecuador), **SV** (El Salvador), **PY** (Paraguay), **PE** (Peru), **UY** (Uruguay)

We keep any matched pair where the subsidiary **or** the parent is in one of these countries. So the dataset includes both inward FDI (foreign parents investing in the region) and outward FDI (Latin American companies owning subsidiaries elsewhere).

---

## Replicating without the proprietary data

The code is general enough to work with other firm-level databases. If you have any two databases with company names and want to link them, you can adapt the pipeline with minimal changes. The Python scripts in `python/` don't depend on Orbis or DNB — they just need a CSV with company names. The Stata dofiles would need path and variable name updates, but the logic is the same.
