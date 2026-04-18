
********************************************************************************
*
*  06_match_parents.do
*
*  Matches DNB ultimate parent companies to Orbis parent companies using
*  fuzzy match results validated by the AI review pipeline.
*
*  This is the parent-company counterpart to 07_final_match_pipeline.do.
*  The affiliate-level match (in 07) links subsidiaries operating in Latin
*  America. This script links the ultimate owners (headquarters) regardless
*  of where they are located, which is important for understanding the
*  origin of FDI flows.
*
*  Parent matching differences from affiliate matching:
*   - Parent names are often holding companies with less standardized naming
*   - The AI review here includes a third question (Q3: ranking) to resolve
*     cases where multiple Orbis parents match the same DNB parent name.
*     We keep the one with the highest Q3 ranking score.
*   - Confidence threshold is the same as the affiliate match (-0.75) because
*     parent-level precision matters more for ownership attribution.
*
*  Match criteria used here:
*    match_final_1:  conf <= -0.75 AND Q1 = "Yes"
*
*  Inputs:
*    fuzzy_match_par_v1_final.csv    (from 02_fuzzy_match_parents.py)
*    postIA parent chunk files       (from AI review)
*    Orbis_DNBformat_v3.dta          (from 01_build_orbis.do)
*
*  Output:
*    Merge_DNB_Orbis_par_PostIA_v2.dta
*
*  Dependencies: 00_config.do
*
*  Author:  Sebastian Velasquez (IADB)
*  Updated: 2025
*
********************************************************************************

do "stata/00_config.do"


********************************************************************************
* === STEP 1: EXPORT PARENT NAME LISTS ===
* (If not done already via 03_prepare_fuzzy_inputs.do)
********************************************************************************

* --- Orbis parent names ---
use "$root/Orbis_DNBformat_v3.dta", clear

keep if ent_name_aff != ""
keep if ent_name_par != ""

duplicates drop ent_name_par, force

keep ent_name_par
rename ent_name_par orbis_name_1

export delimited using "$fuzzy_dir/Orbis_v3_par_v1.csv", replace


* --- DNB parent names ---
import delimited "$dnb_raw", clear

duplicates drop globalultimatebusinessname, force

keep globalultimatebusinessname
rename globalultimatebusinessname DNB_name_1

export delimited using "$fuzzy_dir/DNB_par_v1.csv", replace


********************************************************************************
* === STEP 2: PREPARE CHUNKS FOR AI REVIEW ===
* Split the parent fuzzy match output into 100,000-row chunks.
* Smaller chunks than the affiliate step because parent matching benefits
* from a bit more API budget per pair (tricky holding company names).
********************************************************************************

import delimited "$fuzzy_dir/fuzzy_match_par_v1_final.csv", clear

drop original_name
drop conf

* Rename DNB column to name_par for consistency with the AI review scripts
rename DNB_name_1 name_par

export delimited using "$ia_dir/fuzzy_match_par_v1_preIA.csv", replace

* Create chunk ID (100,000 rows per chunk)
gen long chunk_id = ceil(_n / 100000)
su chunk_id, meanonly
local chunks = r(max)

* AI review prompt for parents also asks Q3 (ranking among duplicates):
*   Q1: Same company or same parent group? (Yes/Non)
*   Q2: Confidence score 1-10
*   Q3: Ranking score (1 = best match) — used to resolve duplicates
*       when multiple Orbis parents matched the same DNB parent

forval i = 1/`chunks' {
    preserve
    keep if chunk_id == `i'
    drop chunk_id
    export delimited using "$ia_dir/fuzzy_match_par_preIA_`i'.csv", replace
    restore
}


********************************************************************************
* === STEP 3: APPEND AI-REVIEWED CHUNKS ===
* Load the 5 post-AI parent files and combine them.
********************************************************************************

use "$ia_agent/fuzzy_match_par_v1_postIA1.dta", clear

forvalues i = 2(1)5 {
    append using "$ia_agent/fuzzy_match_par_v1_postIA`i'.dta"
}

* Re-encode name columns for consistency across chunks
gen name_par_ = name_par
gen matched_name_ = matched_name
drop name_par matched_name
rename name_par_ name_par
rename matched_name_ matched_name

order name_par matched_name

save "$ia_agent/fuzzy_match_par_v1_postIA.dta", replace


********************************************************************************
* === STEP 4: MERGE AI RESULTS WITH FUZZY SCORES ===
********************************************************************************

* Python saves to fuzzy_dir with the _final suffix
import delimited "$fuzzy_dir/fuzzy_match_par_v1_final.csv", clear

* Rename DNB column to align with the postIA files
rename DNB_name_1 name_par

gen name_par_ = name_par
gen matched_name_ = matched_name
drop name_par matched_name
rename name_par_ name_par
rename matched_name_ matched_name

order name_par matched_name

merge 1:1 name_par matched_name using "$ia_agent/fuzzy_match_par_v1_postIA.dta"

drop if _merge == 2
drop _merge


********************************************************************************
* === STEP 5: RESOLVE DUPLICATES USING Q3 RANKING ===
* When the same DNB parent name matched multiple Orbis parent names,
* the AI review assigned a Q3 ranking. We keep only the top-ranked match
* (lowest Q3 value = best match).
********************************************************************************

* If multiple Orbis names match the same DNB parent, keep the best one
bysort name_par (q3): keep if _n == 1


********************************************************************************
* === STEP 6: APPLY MATCH CRITERIA ===
********************************************************************************

gen match_final_1 = 1 if conf <= -0.75 & q1 == "Yes"
recode match_final_1 (.=0)

keep if match_final_1 == 1

save "$ia_agent/fuzzy_match_par_postIA_final_match1.dta", replace


********************************************************************************
* === STEP 7: MERGE WITH ORBIS PARENT DATA ===
********************************************************************************

use "$root/Orbis_DNBformat_v3.dta", clear

keep if ent_name_aff != ""
keep if ent_name_par != ""

duplicates drop name_par, force

* name_par here is the Orbis BvD parent display name.
* In the postIA file, matched_name = Orbis parent name (same thing).
* Rename before the merge so both sides use the same key variable.
rename name_par matched_name

merge 1:1 matched_name using "$ia_agent/fuzzy_match_par_postIA_final_match1.dta"
drop if _merge == 2
drop _merge

* Restore original name for clarity; name_par (DNB) now comes from postIA file
rename matched_name name_par_orbis

* Attach DNB parent attributes — DNB_2025_match_unique is keyed on companyname
* (one row per subsidiary), so deduplicate on globalultimatebusinessname first.
rename name_par globalultimatebusinessname

tempfile dnb_parents
preserve
use "$ia_dir/DNB_2025_match_unique.dta", clear
duplicates drop globalultimatebusinessname, force
save `dnb_parents'
restore

merge m:1 globalultimatebusinessname using `dnb_parents', keepusing(naics_2_h naics_4_h iso2_parent iso3_parent gobalultimateyearstarted globalultimatedunsnumber)
drop if _merge == 2
drop _merge

save "$root/Merge_DNB_Orbis_par_PostIA_v2.dta", replace

di "Parent match complete."
