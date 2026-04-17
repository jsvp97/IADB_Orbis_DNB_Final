********************************************************************************
* 04_ai_review_prep.do
*
* Prepares the fuzzy match output for AI review.
* Splits fuzzy_match_v1.csv into 170,000-row chunks so each AI review run
* is short enough to finish without hitting API time limits, and any
* failed chunk can be re-run without redoing everything.
*
* Input:   $ia_dir/fuzzy_match_v1.csv       (from 01_fuzzy_match_affiliates.py)
* Outputs: $ia_dir/fuzzy_match_v1_preIA_*.csv  (one file per chunk)
*
* Next: run python/ai_review/03_gemini_batch_cooldown.py on each chunk.
*
* Author:  Sebastian Velasquez (IADB)
* Updated: 2025
********************************************************************************

do "stata/00_config.do"

import delimited "$ia_dir/fuzzy_match_v1.csv", clear

* The AI review only needs the two company names — drop everything else
rename companyname name_aff
keep name_aff matched_name

export delimited using "$ia_dir/fuzzy_match_v1_preIA.csv", replace

* Split into 170,000-row chunks
forvalues i=0(170000)1530000 {

    preserve

    gen n = _n
    local m = `i' + 170000

    keep if n > `i' & n <= `m'
    drop n

    export delimited using "$ia_dir/fuzzy_match_v1_preIA_`i'_`m'.csv", replace

    restore

}
