********************************************************************************
* 07_final_match_pipeline.do
*
* Main pipeline for the Orbis–DNB matching project.
*
* Steps:
*   1  Export unique company names from Orbis and DNB to CSV
*   2  [External] python/fuzzy_matching/01_fuzzy_match_affiliates.py
*   3  [External] stata/04_ai_review_prep.do  (split fuzzy output into chunks)
*   4  [External] python/ai_review/03_gemini_batch_cooldown.py  (one per chunk)
*   5  Append all AI-reviewed chunks into one file
*   6  Merge AI results back onto the fuzzy match table
*   7  Define three match criteria (strict / fuzzy-only / AI-only)
*   8  Merge matched companies with full Orbis dataset
*   9  Merge with DNB to get all subsidiary variables
*   10 Standardize NAICS codes and run descriptive checks
*
* Prereqs: run 01_build_orbis.do and 02_build_dnb.do first
*
* Outputs:
*   $ia_dir/Merge_DNB_Orbis_PostIA_v2.dta        (final matched dataset)
*   $ia_agent/fuzzy_match_v1_postIA_final.dta    (fuzzy + AI quality flags)
*
* Author:  Sebastian Velasquez (IADB)
* Updated: 2025
********************************************************************************

do "stata/00_config.do"


* ==============================================================================
* STEP 1 — Export name lists for Python fuzzy matching
* ==============================================================================

* Orbis affiliate names (reference list — Python matches INTO this)
use "$root/Orbis_DNBformat_v3.dta", clear

keep if ent_name_aff != ""
keep if ent_name_par != ""

duplicates drop ent_name_aff, force

keep ent_name_aff
rename ent_name_aff orbis_name_1

save "$fuzzy_dir/Orbis_v3_aff_v1.dta", replace
export delimited using "$fuzzy_dir/Orbis_v3_aff_v1.csv", replace


* DNB affiliate names (query list — Python matches FROM this)
import delimited "$dnb_raw", clear

duplicates drop companyname, force
keep companyname

export delimited using "$fuzzy_dir/DNB_aff_v1.csv", replace


* ==============================================================================
* STEP 2 — Run Python fuzzy matching
* (pause here and run the script below)
*
*   python python/fuzzy_matching/01_fuzzy_match_affiliates.py
*
* Output: $fuzzy_dir/fuzzy_match_v1_final.csv
*   Columns: original_name (DNB), matched_name (Orbis), conf (cosine distance)
*   conf is negative — closer to 0 = better match
* ==============================================================================


* ==============================================================================
* STEP 3 — Split fuzzy output into chunks for AI review
* (pause here and run the script below)
*
*   stata/04_ai_review_prep.do
*
* Output: $ia_dir/fuzzy_match_v1_preIA_*.csv  (one file per 170k rows)
* ==============================================================================


* ==============================================================================
* STEP 4 — Run AI review
* (pause here and run one of the scripts below for each chunk)
*
*   python python/ai_review/03_gemini_batch_cooldown.py   ← recommended
*   python python/ai_review/04_gpt4_batch_cooldown.py     ← alternative
*
* Output: $ia_agent/fuzzy_match_v1_postIA1.dta ... postIA10.dta
* ==============================================================================


* ==============================================================================
* STEP 5 — Append AI-reviewed chunks
* ==============================================================================

use "$ia_agent/fuzzy_match_v1_postIA1.dta", clear

forvalues i=2(1)10 {
    append using "$ia_agent/fuzzy_match_v1_postIA`i'.dta"
}

* Re-encode name columns to ensure consistent string format across chunks
gen name_aff_    = name_aff
gen matched_name_ = matched_name

drop name_aff matched_name

rename name_aff_     name_aff
rename matched_name_ matched_name

order name_aff matched_name

save "$ia_agent/fuzzy_match_v1_postIA.dta", replace


* ==============================================================================
* STEP 6 — Merge AI results back onto the full fuzzy match file
* ==============================================================================

import delimited "$ia_dir/fuzzy_match_v1.csv", clear

gen name_aff_    = name_aff
gen matched_name_ = matched_name

drop name_aff matched_name

rename name_aff_     name_aff
rename matched_name_ matched_name

order name_aff matched_name

merge 1:1 name_aff matched_name using "$ia_agent/fuzzy_match_v1_postIA.dta"

sort _merge name_aff matched_name

* The first 14,170 rows had an alignment issue between the pre-AI CSV and the
* post-AI chunks (row offset introduced during chunking). Fix by pulling Q1/Q2
* from 14,170 rows further down where the correct values landed after append.
replace q1 = q1[_n+14170] in 1/14170
replace q2 = q2[_n+14170] in 1/14170

drop if _merge==2
drop _merge


* ==============================================================================
* STEP 7 — Define match criteria
*
*   match_final_1  →  fuzzy conf ≤ −0.75  AND  AI says "Yes"    [589,345 firms]
*   match_final_2  →  fuzzy conf ≤ −0.75  only                  [656,755 firms]
*   match_final_3  →  AI says "Yes"  only                       [885,349 firms]
*
* For most analyses use match_final_1 (both steps agree).
* ==============================================================================

gen match_final_1 = 1 if conf <= -0.75 & q1 == "Yes"
gen match_final_2 = 1 if conf <= -0.75
gen match_final_3 = 1 if q1 == "Yes"

recode match_final_1 match_final_2 match_final_3 (.=0)

save "$ia_agent/fuzzy_match_v1_postIA_final.dta", replace


* Keep only match_final_1 pairs for the merge steps below
use "$ia_agent/fuzzy_match_v1_postIA_final.dta", clear

keep if match_final_1 == 1

save "$ia_agent/fuzzy_match_v1_postIA_final_match1.dta", replace


* ==============================================================================
* STEP 8 — Merge back onto full Orbis dataset
* ==============================================================================

use "$root/Orbis_DNBformat_v3.dta", clear

keep if ent_name_aff != ""
keep if ent_name_par != ""

duplicates drop name_aff, force

* Attach fuzzy+AI match flag
merge 1:1 name_aff using "$ia_agent/fuzzy_match_v1_postIA_final_match1.dta"

drop if _merge==2
drop _merge

* Attach DNB variables via matched_name (the Orbis name that maps to a DNB firm)
merge m:1 matched_name using "$ia_dir/DNB_2025_match_unique.dta"

save "$ia_dir/Merge_DNB_Orbis_PostIA_v1.dta", replace


* ==============================================================================
* STEP 9 — Standardize NAICS codes
* Convert ISO2 → ISO3, then consolidate NAICS 2-digit codes:
*   31/32/33 → 31 (Manufacturing)
*   44/45    → 44 (Retail Trade)
*   48/49    → 48 (Transportation & Warehousing)
* Recode legacy codes 01–08 (from old SIC crosswalks) to 99.
* ==============================================================================

use "$ia_dir/Merge_DNB_Orbis_PostIA_v1.dta", clear

rename _merge _merge_DNB

kountry iso2_subsidiary, from(iso2c) to(iso3c)
rename _ISO3C_ iso3_subsidiary

kountry iso2_parent, from(iso2c) to(iso3c)
rename _ISO3C_ iso3_parent

* Consolidate NAICS — affiliate (Orbis)
replace naics_aff_2="31" if naics_aff_2=="32"
replace naics_aff_2="31" if naics_aff_2=="33"
replace naics_aff_2="44" if naics_aff_2=="45"
replace naics_aff_2="48" if naics_aff_2=="49"

* Consolidate NAICS — parent (Orbis)
replace naics_par_2="31" if naics_par_2=="32"
replace naics_par_2="31" if naics_par_2=="33"
replace naics_par_2="44" if naics_par_2=="45"
replace naics_par_2="48" if naics_par_2=="49"

* Consolidate NAICS — subsidiary (DNB)
replace naics_2_c="31" if naics_2_c=="32"
replace naics_2_c="31" if naics_2_c=="33"
replace naics_2_c="44" if naics_2_c=="45"
replace naics_2_c="48" if naics_2_c=="49"

* Consolidate NAICS — parent HQ (DNB)
replace naics_2_h="31" if naics_2_h=="32"
replace naics_2_h="31" if naics_2_h=="33"
replace naics_2_h="44" if naics_2_h=="45"
replace naics_2_h="48" if naics_2_h=="49"

* Legacy SIC-mapped codes that don't exist in current NAICS
foreach v in naics_aff_2 naics_par_2 {
    forvalues c=1/8 {
        replace `v'="99" if `v'=="0`c'"
    }
}


* ==============================================================================
* STEP 10 — Descriptive checks
* ==============================================================================

tab naics_aff_2
tab naics_par_2
tab naics_2_c
tab naics_2_h

tab naics_aff_2 if ent_type_par != "I"
tab naics_par_2 if ent_type_par != "I"

tab iso3_subsidiary
tab iso3_parent
tab iso3_aff
tab iso3_par

tab iso3_subsidiary if ent_type_par != "I"
tab iso3_parent     if ent_type_par != "I"

save "$ia_dir/Merge_DNB_Orbis_PostIA_v2.dta", replace

di "Pipeline complete. Final dataset: " _N " rows."

********************************************************************************
* END
********************************************************************************
