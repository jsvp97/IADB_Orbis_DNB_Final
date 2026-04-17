********************************************************************************
* 03_prepare_fuzzy_inputs.do
*
* Exports the four CSV files the Python fuzzy matching scripts need:
*   $fuzzy_dir/Orbis_v3_aff_v1.csv  — Orbis affiliate names (reference)
*   $fuzzy_dir/Orbis_v3_par_v1.csv  — Orbis parent names   (reference)
*   $fuzzy_dir/DNB_aff_v1.csv       — DNB subsidiary names (query)
*   $fuzzy_dir/DNB_par_v1.csv       — DNB parent names     (query)
*
* Both scripts only need unique names, not one row per ownership link,
* so we deduplicate before exporting.
*
* Next: run python/fuzzy_matching/01_fuzzy_match_affiliates.py
*            and python/fuzzy_matching/02_fuzzy_match_parents.py
*
* Author:  Sebastian Velasquez (IADB)
* Updated: 2025
********************************************************************************

do "stata/00_config.do"


* ==============================================================================
* AFFILIATE NAMES
* ==============================================================================

* Orbis affiliate reference list
use "$root/Orbis_DNBformat_v3.dta", clear
keep if ent_name_aff != ""
keep if ent_name_par != ""
duplicates drop ent_name_aff, force
keep ent_name_aff
rename ent_name_aff orbis_name_1
save "$fuzzy_dir/Orbis_v3_aff_v1.dta", replace
export delimited using "$fuzzy_dir/Orbis_v3_aff_v1.csv", replace
di "Orbis affiliates: " _N " unique names"

* DNB subsidiary query list
import delimited "$dnb_raw", clear
duplicates drop companyname, force
keep companyname
export delimited using "$fuzzy_dir/DNB_aff_v1.csv", replace
di "DNB subsidiaries: " _N " unique names"


* ==============================================================================
* PARENT NAMES
* ==============================================================================

* Orbis parent reference list
use "$root/Orbis_DNBformat_v3.dta", clear
keep if ent_name_aff != ""
keep if ent_name_par != ""
duplicates drop ent_name_par, force
keep ent_name_par
rename ent_name_par orbis_name_1
save "$fuzzy_dir/Orbis_v3_par_v1.dta", replace
export delimited using "$fuzzy_dir/Orbis_v3_par_v1.csv", replace
di "Orbis parents: " _N " unique names"

* DNB parent query list
import delimited "$dnb_raw", clear
duplicates drop globalultimatebusinessname, force
keep globalultimatebusinessname
rename globalultimatebusinessname DNB_name_1
export delimited using "$fuzzy_dir/DNB_par_v1.csv", replace
di "DNB parents: " _N " unique names"
