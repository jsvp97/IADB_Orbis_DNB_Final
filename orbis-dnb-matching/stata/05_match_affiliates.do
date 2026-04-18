
********************************************************************************
*
*  05_match_affiliates.do
*
*  Merges DNB 2025 subsidiary data with Orbis affiliate data using the
*  fuzzy match results as a bridge.
*
*  The core idea:
*    DNB has rich information on subsidiaries operating in Latin America
*    (employment, NAICS sector, year started, ultimate parent). Orbis has
*    ownership network data with BvD IDs. By linking the two on company names,
*    we get a dataset that combines the best of both sources.
*
*  This script handles two things:
*    1. Clean the DNB raw data (fix country names, convert to ISO codes,
*       extract NAICS codes, handle missing years)
*    2. Merge with Orbis via the fuzzy match output, then filter to the
*       10 Latin American countries of interest
*
*  Earlier version of the pipeline (before the AI review step was added).
*  The main pipeline with AI review is in 07_final_match_pipeline.do.
*
*  Outputs:
*    DNB_Orbis_matched_v1.dta     (full match, global)
*    DNB_Orbis_2025_10cou.dta     (filtered to 10 Latin American countries)
*
*  Dependencies:
*    - 00_config.do
*    - fuzzy_matched_v2_order.dta  (from Python fuzzy matching, saved by Step A)
*    - Orbis_DNBformat_v3.dta      (from 01_build_orbis.do)
*
*  Author:  Sebastian Velasquez (IADB)
*  Updated: 2025
*
********************************************************************************

do "stata/00_config.do"


* --- Step A: Load and filter the fuzzy match results ---
* We use a confidence cutoff of -0.8 to keep only good matches.
* The score is a cosine distance — closer to 0 means more similar.
import delimited "$ia_dir/fuzzy_match_v2.csv", clear

gen match=1 if conf<-0.8

keep if match==1

save "$fuzzy_dir/fuzzy_matched_v2.dta", replace


* Ordered version (alternative sort order, used in the merge below)
import delimited "$ia_dir/fuzzy_match_v2_order.csv", clear

gen match=1 if conf<-0.8

keep if match==1

save "$fuzzy_dir/fuzzy_matched_v2_order.dta", replace


* --- Step B: Load DNB 2025 and fix country names ---
* The DNB raw file stores multi-word country names without spaces
* (e.g., "HONGKONG" instead of "HONG KONG"). The kountry command needs
* proper names to convert to ISO codes, so we fix them here.
* We apply the same corrections to both countryname (subsidiary location)
* and globalultimatecountry (parent location).

import delimited "$dnb_raw", clear

* Re-attach spaces to country names that were stored without them
replace countryname="CAYMAN ISLANDS" if countryname=="CAYMANISLANDS"
replace countryname="COSTA RICA" if countryname=="COSTARICA"
replace countryname="CZECH REPUBLIC" if countryname=="CZECHREPUBLIC"
replace countryname="DOMINICAN REPUBLIC" if countryname=="DOMINICANREPUBLIC"
replace countryname="EL SALVADOR" if countryname=="ELSALVADOR"
replace countryname="HONG KONG" if countryname=="HONGKONG"
replace countryname="IVORY COAST" if countryname=="IVORYCOAST"
replace countryname="KOREA REP OF" if countryname=="KOREAREPOF"
replace countryname="MARSHALL ISLANDS" if countryname=="MARSHALLISLANDS"
replace countryname="NEW ZEALAND" if countryname=="NEWZEALAND"
replace countryname="NORTHERN IRELAND" if countryname=="NORTHERNIRELAND"
replace countryname="RUSSIAN FEDERATION" if countryname=="RUSSIANFEDERATION"
replace countryname="SAN MARINO" if countryname=="SANMARINO"
replace countryname="SAUDI ARABIA" if countryname=="SAUDIARABIA"
replace countryname="SOUTH AFRICA" if countryname=="SOUTHAFRICA"
replace countryname="ST LUCIA" if countryname=="STLUCIA"
replace countryname="TRINIDAD & TOBAGO" if countryname=="TRINIDAD&TOBAGO"
replace countryname="TURKEY" if countryname=="TURKIYE"
replace countryname="UNITED ARAB EMIRATES" if countryname=="UNITEDARABEMIRATES"
replace countryname="VIRGIN ISLANDS UK" if countryname=="VIRGINISLANDSUK"

* Same corrections for the parent country
replace globalultimatecountry="CAYMAN ISLANDS" if globalultimatecountry=="CAYMANISLANDS"
replace globalultimatecountry="COSTA RICA" if globalultimatecountry=="COSTARICA"
replace globalultimatecountry="CZECH REPUBLIC" if globalultimatecountry=="CZECHREPUBLIC"
replace globalultimatecountry="DOMINICAN REPUBLIC" if globalultimatecountry=="DOMINICANREPUBLIC"
replace globalultimatecountry="EL SALVADOR" if globalultimatecountry=="ELSALVADOR"
replace globalultimatecountry="HONG KONG" if globalultimatecountry=="HONGKONG"
replace globalultimatecountry="IVORY COAST" if globalultimatecountry=="IVORYCOAST"
replace globalultimatecountry="KOREA REP OF" if globalultimatecountry=="KOREAREPOF"
replace globalultimatecountry="MARSHALL ISLANDS" if globalultimatecountry=="MARSHALLISLANDS"
replace globalultimatecountry="NEW ZEALAND" if globalultimatecountry=="NEWZEALAND"
replace globalultimatecountry="NORTHERN IRELAND" if globalultimatecountry=="NORTHERNIRELAND"
replace globalultimatecountry="RUSSIAN FEDERATION" if globalultimatecountry=="RUSSIANFEDERATION"
replace globalultimatecountry="SAN MARINO" if globalultimatecountry=="SANMARINO"
replace globalultimatecountry="SAUDI ARABIA" if globalultimatecountry=="SAUDIARABIA"
replace globalultimatecountry="SOUTH AFRICA" if globalultimatecountry=="SOUTHAFRICA"
replace globalultimatecountry="ST LUCIA" if globalultimatecountry=="STLUCIA"
replace globalultimatecountry="TRINIDAD & TOBAGO" if globalultimatecountry=="TRINIDAD&TOBAGO"
replace globalultimatecountry="TURKEY" if globalultimatecountry=="TURKIYE"
replace globalultimatecountry="UNITED ARAB EMIRATES" if globalultimatecountry=="UNITEDARABEMIRATES"
replace globalultimatecountry="VIRGIN ISLANDS UK" if globalultimatecountry=="VIRGINISLANDSUK"

* Handle territories and disputed areas — map them to the sovereign country
replace globalultimatecountry="NORTH MACEDONIA" if globalultimatecountry=="MACEDONIA"
replace globalultimatecountry="TURKEY" if globalultimatecountry=="TURKISH REP N CYPRUS"
replace globalultimatecountry="TURKEY" if globalultimatecountry=="TURKS & CAICOS IS"

* UK constituent countries → United Kingdom
replace globalultimatecountry="UNITED KINGDOM" if globalultimatecountry=="ENGLAND"
replace globalultimatecountry="UNITED KINGDOM" if globalultimatecountry=="NORTHERN IRELAND"
replace globalultimatecountry="UNITED KINGDOM" if globalultimatecountry=="SCOTLAND"
replace globalultimatecountry="UNITED KINGDOM" if globalultimatecountry=="WALES"

replace countryname="TURKEY" if countryname=="TURKISH REP N CYPRUS"
replace countryname="TURKEY" if countryname=="TURKS & CAICOS IS"
replace countryname="UNITED KINGDOM" if countryname=="ENGLAND"
replace countryname="UNITED KINGDOM" if countryname=="NORTHERN IRELAND"
replace countryname="UNITED KINGDOM" if countryname=="SCOTLAND"
replace countryname="UNITED KINGDOM" if countryname=="WALES"

* Remove domestic relationships (subsidiary and parent both in same country)
* We are only interested in cross-border FDI in this project
drop if countryname=="UNITED KINGDOM" & globalultimatecountry=="UNITED KINGDOM"
drop if countryname=="TURKEY" & globalultimatecountry=="TURKEY"


* --- Step C: Handle year variables ---
* DNB reports yearstarted for the subsidiary and gobalultimateyearstarted
* for the ultimate parent. If the subsidiary year is missing or zero but
* the parent year is available and higher, we use the parent year.
* This is just a data quality fix to avoid gaps in time-series analysis.

replace yearstarted=gobalultimateyearstarted if gobalultimateyearstarted>yearstarted & gobalultimateyearstarted!=. & yearstarted==0
replace yearstarted=gobalultimateyearstarted if gobalultimateyearstarted>yearstarted & gobalultimateyearstarted!=. & yearstarted==.

* Cap early years at 1999 — we don't have reliable coverage before that
replace yearstarted=1999 if yearstarted<1999
replace gobalultimateyearstarted=1999 if gobalultimateyearstarted<1999

* For missing years, we assume the company existed before 2000 and assign 1999.
* This keeps the time-series count consistent rather than losing these firms.
replace yearstarted=1999 if yearstarted==.
replace gobalultimateyearstarted=1999 if gobalultimateyearstarted==.


* --- Step D: Extract NAICS codes ---
* DNB stores the full 6-digit NAICS code. We truncate to 2-digit and 4-digit
* for sector-level analysis.
* _c = subsidiary/company (primary6digitnaicscode)
* _h = global ultimate parent / HQ (globalultimateprimarynaicsco)

tostring globalultimateprimarynaicsco, replace
tostring primary6digitnaicscode, replace

gen naics_2_c=substr(primary6digitnaicscode,1,2)
gen naics_2_h=substr(globalultimateprimarynaicsco,1,2)

* Replace missing values with 99 (unclassified)
replace naics_2_c="99" if naics_2_c=="."
replace naics_2_h="99" if naics_2_h=="."

gen naics_4_c=substr(primary6digitnaicscode,1,4)
gen naics_4_h=substr(globalultimateprimarynaicsco,1,4)

replace naics_4_c="9999" if naics_4_c=="."
replace naics_4_h="9999" if naics_4_h=="."


* --- Step E: Convert country names to ISO codes ---
* We use the kountry Stata package to go from country name → ISO3N → ISO3C/ISO2C.
* Some territories are not in the standard ISO tables, so we patch them manually.

kountry countryname, from(other) stuck
rename _ISO3N_ iso3n
kountry iso3n, from(iso3n) to(iso3c)
rename _ISO3C_ iso3_aff
kountry iso3n, from(iso3n) to(iso2c)
rename _ISO2C_ iso2_aff

* Manual patches for territories not in standard ISO tables
replace iso3_aff="BES" if countryname=="BONAIRE ST EUST SABA"
replace iso3_aff="CAF" if countryname=="CENTRAL AFRICAN REP"
replace iso3_aff="COD" if countryname=="CONGO DEMOCRATIC REP"
replace iso3_aff="CUW" if countryname=="CURACAO"
replace iso3_aff="SWZ" if countryname=="ESWATINI"
replace iso3_aff="KSV" if countryname=="KOSOVO"
replace iso3_aff="FSM" if countryname=="MICRONESIA FED ST"
replace iso3_aff="MKD" if countryname=="NORTH MACEDONIA"
replace iso3_aff="NFK" if countryname=="NORFOLK ISLAND"
replace iso3_aff="MNP" if countryname=="NORTHERN MARIANA IS"
replace iso3_aff="SXM" if countryname=="ST MAARTEN"
replace iso3_aff="VCT" if countryname=="ST VINCENT"

replace iso2_aff = "BQ" if countryname == "BONAIRE ST EUST SABA"
replace iso2_aff = "CF" if countryname == "CENTRAL AFRICAN REP"
replace iso2_aff = "CD" if countryname == "CONGO DEMOCRATIC REP"
replace iso2_aff = "CW" if countryname == "CURACAO"
replace iso2_aff = "SZ" if countryname == "ESWATINI"
replace iso2_aff = "XK" if countryname == "KOSOVO"
replace iso2_aff = "FM" if countryname == "MICRONESIA FED ST"
replace iso2_aff = "MK" if countryname == "NORTH MACEDONIA"
replace iso2_aff = "NF" if countryname == "NORFOLK ISLAND"
replace iso2_aff = "MP" if countryname == "NORTHERN MARIANA IS"
replace iso2_aff = "SX" if countryname == "ST MAARTEN"
replace iso2_aff = "VC" if countryname == "ST VINCENT"

drop iso3n

* Repeat for the parent country (globalultimatecountry → iso3_par, iso2_par)
kountry globalultimatecountry, from(other) stuck
rename _ISO3N_ iso3n
kountry iso3n, from(iso3n) to(iso3c)
rename _ISO3C_ iso3_par
kountry iso3n, from(iso3n) to(iso2c)
rename _ISO2C_ iso2_par

replace iso3_par="BES" if globalultimatecountry=="BONAIRE ST EUST SABA"
replace iso3_par="CAF" if globalultimatecountry=="CENTRAL AFRICAN REP"
replace iso3_par="COD" if globalultimatecountry=="CONGO DEMOCRATIC REP"
replace iso3_par="CUW" if globalultimatecountry=="CURACAO"
replace iso3_par="SWZ" if globalultimatecountry=="ESWATINI"
replace iso3_par="KSV" if globalultimatecountry=="KOSOVO"
replace iso3_par="FSM" if globalultimatecountry=="MICRONESIA FED ST"
replace iso3_par="MKD" if globalultimatecountry=="NORTH MACEDONIA"
replace iso3_par="NFK" if globalultimatecountry=="NORFOLK ISLAND"
replace iso3_par="MNP" if globalultimatecountry=="NORTHERN MARIANA IS"
replace iso3_par="SXM" if globalultimatecountry=="ST MAARTEN"
replace iso3_par="VCT" if globalultimatecountry=="ST VINCENT"

replace iso2_par = "BQ" if globalultimatecountry == "BONAIRE ST EUST SABA"
replace iso2_par = "CF" if globalultimatecountry == "CENTRAL AFRICAN REP"
replace iso2_par = "CD" if globalultimatecountry == "CONGO DEMOCRATIC REP"
replace iso2_par = "CW" if globalultimatecountry == "CURACAO"
replace iso2_par = "SZ" if globalultimatecountry == "ESWATINI"
replace iso2_par = "XK" if globalultimatecountry == "KOSOVO"
replace iso2_par = "FM" if globalultimatecountry == "MICRONESIA FED ST"
replace iso2_par = "MK" if globalultimatecountry == "NORTH MACEDONIA"
replace iso2_par = "NF" if globalultimatecountry == "NORFOLK ISLAND"
replace iso2_par = "MP" if globalultimatecountry == "NORTHERN MARIANA IS"
replace iso2_par = "SX" if globalultimatecountry == "ST MAARTEN"
replace iso2_par = "VC" if globalultimatecountry == "ST VINCENT"

drop iso3n


* --- Step F: Merge with Orbis via fuzzy match ---
* The fuzzy match file provides a bridge: DNB company name → Orbis company name.
* We first merge DNB on the DNB company name (m:1 because one DNB name can have
* multiple rows), then rename the matched Orbis name and merge again on Orbis.

merge m:1 companyname using "$fuzzy_dir/fuzzy_matched_v2_order.dta"

drop if _merge==2

rename _merge _merge_1

* The matched_name column in the fuzzy output is the Orbis affiliate name
rename matched_name name_aff

* Merge with the Orbis ownership data on the Orbis affiliate name
merge m:1 name_aff using "$root/Orbis_DNBformat_v3.dta"
drop if _merge==2
drop _merge

* Extract 4-digit NAICS from Orbis 6-digit codes
gen naics_aff_4=substr(naics_aff_6,1,4)
gen naics_par_4=substr(naics_par_6,1,4)

tostring yearstarted gobalultimateyearstarted, replace


* --- Step G: Build harmonized variables ---
* We create unified variables that pull from DNB if available
* and fall back to Orbis if DNB doesn't have the information.
* This gives us the most complete coverage possible.

gen company_name=companyname if _merge_1==1 | _merge_1==3
replace company_name=name_aff if company_name==""

gen company_country=iso2_aff
replace company_country=iso2_subsidiary if company_country==""

gen parent_name=globalultimatebusinessname if _merge_1==1 | _merge_1==3
replace parent_name=name_par if parent_name==""

gen parent_country=iso2_par
replace parent_country=iso2_parent if parent_country==""

gen company_naics_4=naics_4_c if _merge_1==1 | _merge_1==3
replace company_naics_4=naics_aff_4 if company_naics_4==""

gen parent_naics_4=naics_4_h if _merge_1==1 | _merge_1==3
replace parent_naics_4=naics_par_4 if parent_naics_4==""

gen company_yearstarted=yearstarted if _merge_1==1 | _merge_1==3
replace company_yearstarted=year_incorp_aff if company_yearstarted==""

gen parent_yearstarted=gobalultimateyearstarted if _merge_1==1 | _merge_1==3
replace parent_yearstarted=year_incorp_par if parent_yearstarted==""

* Track which database the observation came from
gen database="DNB" if _merge_1==1 | _merge_1==3
replace database="Orbis" if database==""

* Keep only the final harmonized variables
keep company_name company_country parent_name parent_country company_naics_4 parent_naics_4 company_yearstarted parent_yearstarted database dunsnumber globalultimatedunsnumber subsidiarybvdid guo25

order company_name company_country parent_name parent_country company_naics_4 parent_naics_4 company_yearstarted parent_yearstarted database dunsnumber globalultimatedunsnumber subsidiarybvdid guo25

save "$root/DNB_Orbis_matched_v1.dta", replace


* --- Step H: Filter to the 10 Latin American countries ---
* The IDB research focus is on multinationals operating in Latin America.
* We keep any observation where the subsidiary OR the parent is in one of
* the 10 countries: Argentina, Chile, Colombia, Costa Rica, Dominican Republic,
* Ecuador, El Salvador, Paraguay, Peru, Uruguay.

gen aff_10_cou=1 if company_country=="AR" | company_country=="CL" | company_country=="CO" | company_country=="CR" | company_country=="DO" | company_country=="EC" | company_country=="SV" | company_country=="PY" | company_country=="PE" | company_country=="UY"

gen par_10_cou=1 if parent_country=="AR" | parent_country=="CL" | parent_country=="CO" | parent_country=="CR" | parent_country=="DO" | parent_country=="EC" | parent_country=="SV" | parent_country=="PY" | parent_country=="PE" | parent_country=="UY"

recode aff_10_cou par_10_cou (.=0)

* Keep if subsidiary OR parent is in one of the 10 countries
keep if aff_10_cou==1 | par_10_cou==1

save "$root/DNB_Orbis_2025_10cou.dta", replace