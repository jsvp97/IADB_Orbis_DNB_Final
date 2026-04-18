
********************************************************************************
*
*  08_descriptive_analysis.do
*
*  Generates descriptive statistics, aggregated datasets, and world maps
*  from the Orbis database. This script characterizes the distribution of
*  multinationals before and after the matching process.
*
*  What this covers:
*    1. Sector standardization — harmonize NAICS/USSIC codes
*    2. Aggregate counts by country, sector, and year
*    3. Cumulative time series (how multinational activity built up over time)
*    4. World maps of affiliate and parent distributions
*
*  The maps use the spmap Stata package with a Web Mercator projection.
*  Color intensity represents the count of affiliates/parents per country,
*  using a heat palette with custom breaks.
*
*  Outputs (stored in $root/Analysis/):
*    iso2_year.dta              ← affiliate counts by country × year
*    iso2_ussic2_year.dta       ← by country × sector × year
*    orbis_sector_year.dta      ← by sector × year
*    ... and several other slices
*    World maps as PNG files (if spmap is installed)
*
*  Dependencies:
*    - 00_config.do
*    - 01_build_orbis.do         (must run first)
*    - spmap package:  ssc install spmap
*    - shp2dta package: ssc install shp2dta (for world shapefile)
*
*  Author:  Sebastian Velasquez (IADB)
*  Updated: 2025
*
********************************************************************************

do "stata/00_config.do"
cap mkdir "$root/Analysis"


********************************************************************************
* === PART 1: SECTOR STANDARDIZATION ===
*
* We work with two industry classification systems:
*   NAICS — North American Industry Classification System
*   USSIC — US Standard Industrial Classification (older system, still common)
*
* For both, we extract 2-digit and 4-digit codes from the 6-digit codes.
* The USSIC 2-digit codes are then mapped to broad sector letter categories
* (A through V) following the standard SIC section structure.
*
* NAICS consolidation (same rules as in 07_final_match_pipeline.do):
*   31/32/33 → 31  (Manufacturing)
*   44/45    → 44  (Retail Trade)
*   48/49    → 48  (Transportation & Warehousing)
********************************************************************************

use "$root/Orbis_DNBformat_v3.dta", clear

* --- 2-digit NAICS ---
replace naics_aff_2 = "31" if naics_aff_2 == "32"
replace naics_aff_2 = "31" if naics_aff_2 == "33"
replace naics_aff_2 = "44" if naics_aff_2 == "45"
replace naics_aff_2 = "48" if naics_aff_2 == "49"

replace naics_par_2 = "31" if naics_par_2 == "32"
replace naics_par_2 = "31" if naics_par_2 == "33"
replace naics_par_2 = "44" if naics_par_2 == "45"
replace naics_par_2 = "48" if naics_par_2 == "49"

* --- USSIC letter categories ---
* Map 2-digit USSIC codes to the standard SIC division letters.
* This gives cleaner sector labels for charts and tables.
*
* Division A  (01–09):  Agriculture, Forestry, Fishing
* Division B  (10–14):  Mining
* Division C  (15–17):  Construction
* Division D  (20–39):  Manufacturing
* Division E  (40–49):  Transportation, Communications, Electric/Gas/Sanitary
* Division F  (50–51):  Wholesale Trade
* Division G  (52–59):  Retail Trade
* Division H  (60–67):  Finance, Insurance, Real Estate
* Division I  (70–89):  Services
* Division J  (91–99):  Public Administration

gen ussic2 = substr(ussic_aff_6, 1, 2)
destring ussic2, replace force

gen sector_cat = ""
replace sector_cat = "A" if ussic2 >= 1  & ussic2 <= 9
replace sector_cat = "B" if ussic2 >= 10 & ussic2 <= 14
replace sector_cat = "C" if ussic2 >= 15 & ussic2 <= 17
replace sector_cat = "D" if ussic2 >= 20 & ussic2 <= 39
replace sector_cat = "E" if ussic2 >= 40 & ussic2 <= 49
replace sector_cat = "F" if ussic2 >= 50 & ussic2 <= 51
replace sector_cat = "G" if ussic2 >= 52 & ussic2 <= 59
replace sector_cat = "H" if ussic2 >= 60 & ussic2 <= 67
replace sector_cat = "I" if ussic2 >= 70 & ussic2 <= 89
replace sector_cat = "J" if ussic2 >= 91 & ussic2 <= 99
replace sector_cat = "Z" if sector_cat == ""    // unclassified


********************************************************************************
* === PART 2: AGGREGATE BY COUNTRY ===
* Count affiliates and parents per ISO2 country code.
* The cumulative version tracks how the stock of multinationals
* grew over time — useful for time-series charts and maps.
********************************************************************************

* Simple count by country
preserve
collapse (count) n_aff=ent_name_aff, by(iso2_aff)
rename iso2_aff iso2
save "$root/Analysis/iso2_aff.dta", replace
restore

* Count by country × year with cumulative running total
preserve
collapse (count) n_aff=ent_name_aff, by(iso2_aff year_incorp_aff)
rename iso2_aff iso2
rename year_incorp_aff year

* Cumulative count: how many affiliates existed in each country up to year t
sort iso2 year
by iso2: gen cum_aff = sum(n_aff)

save "$root/Analysis/iso2_year.dta", replace
restore


********************************************************************************
* === PART 3: AGGREGATE BY SECTOR ===
********************************************************************************

preserve
collapse (count) n_aff=ent_name_aff, by(naics_aff_2)
save "$root/Analysis/naics2_aff.dta", replace
restore

* Country × sector × year (the most granular slice)
preserve
collapse (count) n_aff=ent_name_aff, by(iso2_aff naics_aff_2 year_incorp_aff)
rename iso2_aff iso2
rename year_incorp_aff year

sort iso2 naics_aff_2 year
by iso2 naics_aff_2: gen cum_aff = sum(n_aff)

save "$root/Analysis/iso2_ussic2_year.dta", replace
restore


********************************************************************************
* === PART 4: WORLD MAPS ===
*
* We create choropleth maps showing affiliate counts by country.
* The maps use spmap with a natural-break color scheme.
*
* Before running this section, you need a world shapefile in Stata format.
* One option: download the Natural Earth shapefile from naturalearthdata.com
* and convert it using shp2dta:
*
*   shp2dta using "ne_110m_admin_0_countries.shp", ///
*       database("world_db") coordinates("world_coords") replace
*
* The code below assumes those files are in $root/Shapefiles/.
********************************************************************************

use "$root/Analysis/iso2_year.dta", clear

* Keep most recent year for the map
keep if year == 2023

* Merge with shapefile data
merge 1:1 iso2 using "$root/Shapefiles/world_db.dta"

* Map: affiliate counts per country
* Color range: white (0) to dark red (max), 7 quantile breaks
spmap cum_aff using "$root/Shapefiles/world_coords.dta", ///
    id(_ID) ///
    fcolor(Heat) ///
    ocolor(white ..) ///
    osize(0.02 ..) ///
    legend(position(5) size(small)) ///
    title("Affiliate locations — Orbis 2023", size(medium))

graph export "$root/Analysis/map_affiliates_2023.png", replace width(2400)

di "Descriptive analysis complete. Outputs saved to $root/Analysis/"
