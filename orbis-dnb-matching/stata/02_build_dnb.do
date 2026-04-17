********************************************************************************
* 02_build_dnb.do
*
* Cleans the Dun & Bradstreet 2025 export and prepares it for matching.
*
*   1. Fixes country names that DNB stored without spaces (e.g., "HONGKONG")
*   2. Converts country names to ISO2 and ISO3 codes via kountry
*   3. Manually patches territories not in the standard ISO lookup table
*   4. Cleans year variables (missing/zero → 1999, pre-1999 → 1999)
*   5. Extracts 2-digit and 4-digit NAICS codes
*
* Outputs:
*   $ia_dir/DNB_2025_match_unique.dta  — one row per unique company name
*   $root/Affiliates_DNB.dta           — all subsidiaries
*   $root/DNB_maps_2023.dta            — counts by country × year (for maps)
*
* Requires: kountry (ssc install kountry)
* Author:   Sebastian Velasquez (IADB)
* Updated:  2025
********************************************************************************

do "stata/00_config.do"


* ==============================================================================
* PART 1 — FIX COUNTRY NAMES
* DNB concatenates multi-word country names. kountry can't find "HONGKONG"
* but can find "HONG KONG". Fix both the subsidiary and the parent columns.
* ==============================================================================

import delimited "$dnb_raw", clear

* Subsidiary country fixes
replace countryname="CAYMAN ISLANDS"       if countryname=="CAYMANISLANDS"
replace countryname="COSTA RICA"           if countryname=="COSTARICA"
replace countryname="CZECH REPUBLIC"       if countryname=="CZECHREPUBLIC"
replace countryname="DOMINICAN REPUBLIC"   if countryname=="DOMINICANREPUBLIC"
replace countryname="EL SALVADOR"          if countryname=="ELSALVADOR"
replace countryname="HONG KONG"            if countryname=="HONGKONG"
replace countryname="IVORY COAST"          if countryname=="IVORYCOAST"
replace countryname="KOREA REP OF"         if countryname=="KOREAREPOF"
replace countryname="MARSHALL ISLANDS"     if countryname=="MARSHALLISLANDS"
replace countryname="NEW ZEALAND"          if countryname=="NEWZEALAND"
replace countryname="NORTHERN IRELAND"     if countryname=="NORTHERNIRELAND"
replace countryname="RUSSIAN FEDERATION"   if countryname=="RUSSIANFEDERATION"
replace countryname="SAN MARINO"           if countryname=="SANMARINO"
replace countryname="SAUDI ARABIA"         if countryname=="SAUDIARABIA"
replace countryname="SOUTH AFRICA"         if countryname=="SOUTHAFRICA"
replace countryname="ST LUCIA"             if countryname=="STLUCIA"
replace countryname="TRINIDAD & TOBAGO"    if countryname=="TRINIDAD&TOBAGO"
replace countryname="TURKEY"               if countryname=="TURKIYE"
replace countryname="UNITED ARAB EMIRATES" if countryname=="UNITEDARABEMIRATES"
replace countryname="VIRGIN ISLANDS UK"    if countryname=="VIRGINISLANDSUK"

* Parent country fixes (same list)
replace globalultimatecountry="CAYMAN ISLANDS"       if globalultimatecountry=="CAYMANISLANDS"
replace globalultimatecountry="COSTA RICA"           if globalultimatecountry=="COSTARICA"
replace globalultimatecountry="CZECH REPUBLIC"       if globalultimatecountry=="CZECHREPUBLIC"
replace globalultimatecountry="DOMINICAN REPUBLIC"   if globalultimatecountry=="DOMINICANREPUBLIC"
replace globalultimatecountry="EL SALVADOR"          if globalultimatecountry=="ELSALVADOR"
replace globalultimatecountry="HONG KONG"            if globalultimatecountry=="HONGKONG"
replace globalultimatecountry="IVORY COAST"          if globalultimatecountry=="IVORYCOAST"
replace globalultimatecountry="KOREA REP OF"         if globalultimatecountry=="KOREAREPOF"
replace globalultimatecountry="MARSHALL ISLANDS"     if globalultimatecountry=="MARSHALLISLANDS"
replace globalultimatecountry="NEW ZEALAND"          if globalultimatecountry=="NEWZEALAND"
replace globalultimatecountry="NORTHERN IRELAND"     if globalultimatecountry=="NORTHERNIRELAND"
replace globalultimatecountry="RUSSIAN FEDERATION"   if globalultimatecountry=="RUSSIANFEDERATION"
replace globalultimatecountry="SAN MARINO"           if globalultimatecountry=="SANMARINO"
replace globalultimatecountry="SAUDI ARABIA"         if globalultimatecountry=="SAUDIARABIA"
replace globalultimatecountry="SOUTH AFRICA"         if globalultimatecountry=="SOUTHAFRICA"
replace globalultimatecountry="ST LUCIA"             if globalultimatecountry=="STLUCIA"
replace globalultimatecountry="TRINIDAD & TOBAGO"    if globalultimatecountry=="TRINIDAD&TOBAGO"
replace globalultimatecountry="TURKEY"               if globalultimatecountry=="TURKIYE"
replace globalultimatecountry="UNITED ARAB EMIRATES" if globalultimatecountry=="UNITEDARABEMIRATES"
replace globalultimatecountry="VIRGIN ISLANDS UK"    if globalultimatecountry=="VIRGINISLANDSUK"

* Remap UK constituent nations and Turkish-controlled territories to sovereign country
replace globalultimatecountry="UNITED KINGDOM" if inlist(globalultimatecountry,"ENGLAND","SCOTLAND","WALES","NORTHERN IRELAND")
replace countryname="UNITED KINGDOM"           if inlist(countryname,"ENGLAND","SCOTLAND","WALES","NORTHERN IRELAND")
replace globalultimatecountry="TURKEY"         if inlist(globalultimatecountry,"TURKISH REP N CYPRUS","TURKS & CAICOS IS")
replace countryname="TURKEY"                   if inlist(countryname,"TURKISH REP N CYPRUS","TURKS & CAICOS IS")
replace globalultimatecountry="NORTH MACEDONIA" if globalultimatecountry=="MACEDONIA"


* ==============================================================================
* PART 2 — YEAR VARIABLES
* Cap at 1999: coverage before that is too thin for time-series analysis.
* If the subsidiary year is missing but the parent year is known, use the parent.
* ==============================================================================

replace yearstarted = gobalultimateyearstarted if ///
    gobalultimateyearstarted > yearstarted & ///
    gobalultimateyearstarted != . & ///
    yearstarted == 0

replace yearstarted = gobalultimateyearstarted if ///
    gobalultimateyearstarted > yearstarted & ///
    gobalultimateyearstarted != . & ///
    yearstarted == .

replace yearstarted              = 1999 if yearstarted < 1999
replace gobalultimateyearstarted = 1999 if gobalultimateyearstarted < 1999
replace yearstarted              = 1999 if yearstarted == .
replace gobalultimateyearstarted = 1999 if gobalultimateyearstarted == .


* ==============================================================================
* PART 3 — NAICS CODES
* _c = subsidiary (company country), _h = parent (HQ country)
* ==============================================================================

tostring globalultimateprimarynaicsco, replace
tostring primary6digitnaicscode, replace

gen naics_2_c = substr(globalultimateprimarynaicsco, 1, 2)
gen naics_2_h = substr(primary6digitnaicscode, 1, 2)
gen naics_4_c = substr(globalultimateprimarynaicsco, 1, 4)
gen naics_4_h = substr(primary6digitnaicscode, 1, 4)

replace naics_2_c = "99"   if naics_2_c == "."
replace naics_2_h = "99"   if naics_2_h == "."
replace naics_4_c = "9999" if naics_4_c == "."
replace naics_4_h = "9999" if naics_4_h == "."


* ==============================================================================
* PART 4 — ISO CODE CONVERSION
* kountry needs "other" mode (country name → ISO3N) as an intermediate step,
* then converts to ISO3C and ISO2C. Some territories are not in the standard
* ISO table and need manual patches afterward.
* ==============================================================================

kountry countryname, from(other) stuck
rename _ISO3N_ iso3n
kountry iso3n, from(iso3n) to(iso3c)
rename _ISO3C_ iso3_subsidiary
kountry iso3n, from(iso3n) to(iso2c)
rename _ISO2C_ iso2_subsidiary
drop iso3n

kountry globalultimatecountry, from(other) stuck
rename _ISO3N_ iso3n
kountry iso3n, from(iso3n) to(iso3c)
rename _ISO3C_ iso3_parent
kountry iso3n, from(iso3n) to(iso2c)
rename _ISO2C_ iso2_parent
drop iso3n

* Manual ISO3 patches — subsidiary
replace iso3_subsidiary = "BES" if countryname == "BONAIRE ST EUST SABA"
replace iso3_subsidiary = "CAF" if countryname == "CENTRAL AFRICAN REP"
replace iso3_subsidiary = "COD" if countryname == "CONGO DEMOCRATIC REP"
replace iso3_subsidiary = "CUW" if countryname == "CURACAO"
replace iso3_subsidiary = "SWZ" if countryname == "ESWATINI"
replace iso3_subsidiary = "KSV" if countryname == "KOSOVO"
replace iso3_subsidiary = "FSM" if countryname == "MICRONESIA FED ST"
replace iso3_subsidiary = "MKD" if countryname == "NORTH MACEDONIA"
replace iso3_subsidiary = "SXM" if countryname == "ST MAARTEN"
replace iso3_subsidiary = "VCT" if countryname == "ST VINCENT"

replace iso2_subsidiary = "BQ" if countryname == "BONAIRE ST EUST SABA"
replace iso2_subsidiary = "CF" if countryname == "CENTRAL AFRICAN REP"
replace iso2_subsidiary = "CD" if countryname == "CONGO DEMOCRATIC REP"
replace iso2_subsidiary = "CW" if countryname == "CURACAO"
replace iso2_subsidiary = "SZ" if countryname == "ESWATINI"
replace iso2_subsidiary = "XK" if countryname == "KOSOVO"
replace iso2_subsidiary = "FM" if countryname == "MICRONESIA FED ST"
replace iso2_subsidiary = "MK" if countryname == "NORTH MACEDONIA"
replace iso2_subsidiary = "SX" if countryname == "ST MAARTEN"
replace iso2_subsidiary = "VC" if countryname == "ST VINCENT"

* Manual ISO3 patches — parent
replace iso3_parent = "BES" if globalultimatecountry == "BONAIRE ST EUST SABA"
replace iso3_parent = "CAF" if globalultimatecountry == "CENTRAL AFRICAN REP"
replace iso3_parent = "COD" if globalultimatecountry == "CONGO DEMOCRATIC REP"
replace iso3_parent = "CUW" if globalultimatecountry == "CURACAO"
replace iso3_parent = "SWZ" if globalultimatecountry == "ESWATINI"
replace iso3_parent = "KSV" if globalultimatecountry == "KOSOVO"
replace iso3_parent = "FSM" if globalultimatecountry == "MICRONESIA FED ST"
replace iso3_parent = "MKD" if globalultimatecountry == "NORTH MACEDONIA"
replace iso3_parent = "SXM" if globalultimatecountry == "ST MAARTEN"
replace iso3_parent = "VCT" if globalultimatecountry == "ST VINCENT"

replace iso2_parent = "BQ" if globalultimatecountry == "BONAIRE ST EUST SABA"
replace iso2_parent = "CF" if globalultimatecountry == "CENTRAL AFRICAN REP"
replace iso2_parent = "CD" if globalultimatecountry == "CONGO DEMOCRATIC REP"
replace iso2_parent = "CW" if globalultimatecountry == "CURACAO"
replace iso2_parent = "SZ" if globalultimatecountry == "ESWATINI"
replace iso2_parent = "XK" if globalultimatecountry == "KOSOVO"
replace iso2_parent = "FM" if globalultimatecountry == "MICRONESIA FED ST"
replace iso2_parent = "MK" if globalultimatecountry == "NORTH MACEDONIA"
replace iso2_parent = "SX" if globalultimatecountry == "ST MAARTEN"
replace iso2_parent = "VC" if globalultimatecountry == "ST VINCENT"


* ==============================================================================
* PART 5 — SAVE OUTPUTS
* ==============================================================================

* One row per unique DNB company name (used in matching pipeline)
preserve
duplicates drop companyname, force
keep companyname naics_2_c naics_2_h naics_4_c naics_4_h ///
     iso2_subsidiary iso2_parent iso3_subsidiary iso3_parent ///
     yearstarted gobalultimateyearstarted ///
     dunsnumber globalultimatedunsnumber globalultimatebusinessname
save "$ia_dir/DNB_2025_match_unique.dta", replace
restore

* Full affiliate file
save "$root/Affiliates_DNB.dta", replace

* Country × year counts for the map visualizations in 08_descriptive_analysis.do
collapse (count) n_aff=companyname, by(iso2_subsidiary yearstarted)
rename iso2_subsidiary iso2
rename yearstarted     year
save "$root/DNB_maps_2023.dta", replace

di "DNB build complete."
