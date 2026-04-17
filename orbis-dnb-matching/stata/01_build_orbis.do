********************************************************************************
* 01_build_orbis.do
*
* Builds the Orbis working dataset from Bureau van Dijk bulk delivery files.
* Reads five raw text files in chunks (the full Orbis is several GB),
* joins them into one file with affiliate-parent ownership links.
*
* Output: Orbis_DNBformat_v3.dta
*   ~16.9M affiliate records, ~13M parent records
*   Variables: ent_name_aff, ent_name_par, iso2/iso3, naics_aff_6, naics_par_6,
*              year_incorp_aff, bvdid_aff, bvdid_par
*
* Requires: chunky (ssc install chunky), kountry (ssc install kountry)
* Author:   Sebastian Velasquez (IADB)
* Updated:  2025
********************************************************************************

do "stata/00_config.do"


* ==============================================================================
* PART 1 — OWNERSHIP LINKS
* Keep only Global Ultimate Owner relationships (GUO 25/25C/50/50C and HQ).
* These trace ownership up to the entity that ultimately controls each firm.
* ==============================================================================

chunky using "$orbis_raw/Links_current.txt", ///
    chunksize(8000000) ///
    do({
        keep if inlist(relationship, "GUO 25","GUO 25C","GUO 50","GUO 50C","HQ")
        save "$root/links_chunk_`chunknum'.dta", replace
    })

clear
local n_chunks = r(nchunks)
forvalues i = 1/`n_chunks' {
    append using "$root/links_chunk_`i'.dta"
    erase "$root/links_chunk_`i'.dta"
}

keep entity_id linked_entity_id relationship
save "$root/Orbis_links.dta", replace


* ==============================================================================
* PART 2 — ENTITY NAMES AND TYPES
* ==============================================================================

chunky using "$orbis_raw/Entities.txt", ///
    chunksize(8000000) ///
    do({
        keep entity_id name_internat entity_type iso2_country
        save "$root/entities_chunk_`chunknum'.dta", replace
    })

clear
local n_chunks = r(nchunks)
forvalues i = 1/`n_chunks' {
    append using "$root/entities_chunk_`i'.dta"
    erase "$root/entities_chunk_`i'.dta"
}

rename name_internat ent_name
rename entity_type   ent_type
rename iso2_country  iso2

save "$root/Orbis_entities.dta", replace


* ==============================================================================
* PART 3 — INDUSTRY CLASSIFICATIONS
* Keep only the primary code (rank==1) to avoid duplicates per company.
* ==============================================================================

chunky using "$orbis_raw/Industry_classifications.txt", ///
    chunksize(8000000) ///
    do({
        keep if rank==1
        keep entity_id naics ussic
        save "$root/industry_chunk_`chunknum'.dta", replace
    })

clear
local n_chunks = r(nchunks)
forvalues i = 1/`n_chunks' {
    append using "$root/industry_chunk_`i'.dta"
    erase "$root/industry_chunk_`i'.dta"
}

rename naics naics_6
rename ussic  ussic_6

save "$root/Orbis_industry.dta", replace


* ==============================================================================
* PART 4 — IDENTIFIERS (BvD display name, identifier type 7)
* ==============================================================================

chunky using "$orbis_raw/Identifiers.txt", ///
    chunksize(8000000) ///
    do({
        keep if identifier_type==7
        keep entity_id identifier
        save "$root/identifiers_chunk_`chunknum'.dta", replace
    })

clear
local n_chunks = r(nchunks)
forvalues i = 1/`n_chunks' {
    append using "$root/identifiers_chunk_`i'.dta"
    erase "$root/identifiers_chunk_`i'.dta"
}

rename identifier bvd_name
save "$root/Orbis_identifiers.dta", replace


* ==============================================================================
* PART 5 — ASSEMBLE
* Links file is the spine. Merge entity info, industry, and identifier name
* for both the affiliate (entity_id) and the parent (linked_entity_id).
* ==============================================================================

use "$root/Orbis_links.dta", clear

* Affiliate
rename entity_id bvdid_aff
merge m:1 bvdid_aff using "$root/Orbis_entities.dta", keepusing(ent_name ent_type iso2)
drop if _merge==2
drop _merge
rename ent_name ent_name_aff
rename ent_type ent_type_aff
rename iso2     iso2_aff

merge m:1 bvdid_aff using "$root/Orbis_industry.dta", keepusing(naics_6 ussic_6)
drop if _merge==2
drop _merge
rename naics_6 naics_aff_6
rename ussic_6 ussic_aff_6

merge m:1 bvdid_aff using "$root/Orbis_identifiers.dta", keepusing(bvd_name)
drop if _merge==2
drop _merge
rename bvd_name name_aff

* Parent
rename linked_entity_id bvdid_par
merge m:1 bvdid_par using "$root/Orbis_entities.dta", keepusing(ent_name ent_type iso2)
drop if _merge==2
drop _merge
rename ent_name ent_name_par
rename ent_type ent_type_par
rename iso2     iso2_par

merge m:1 bvdid_par using "$root/Orbis_industry.dta", keepusing(naics_6 ussic_6)
drop if _merge==2
drop _merge
rename naics_6 naics_par_6
rename ussic_6 ussic_par_6

merge m:1 bvdid_par using "$root/Orbis_identifiers.dta", keepusing(bvd_name)
drop if _merge==2
drop _merge
rename bvd_name name_par

* 2-digit NAICS
gen naics_aff_2 = substr(naics_aff_6, 1, 2)
gen naics_par_2 = substr(naics_par_6, 1, 2)

replace naics_aff_2 = "99" if naics_aff_2 == "" | naics_aff_2 == "."
replace naics_par_2 = "99" if naics_par_2 == "" | naics_par_2 == "."

* ISO2 → ISO3
kountry iso2_aff, from(iso2c) to(iso3c)
rename _ISO3C_ iso3_aff

kountry iso2_par, from(iso2c) to(iso3c)
rename _ISO3C_ iso3_par

save "$root/Orbis_DNBformat_v3.dta", replace

di "Done. Orbis dataset: " _N " rows."
