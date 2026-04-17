********************************************************************************
* 00_config.do
*
* Run this before any other dofile.
* Edit the three paths below to match your machine — that is all you need to change.
*
* Author:  Sebastian Velasquez (IADB)
* Updated: 2025
********************************************************************************

clear all
set more off

* --- Edit these three lines ---
global root       "C:/Sebas BID/Orbis_DNB"
global orbis_raw  "C:/Sebas BID/Orbis_raw"
global dnb_raw    "C:/Sebas BID/DNB 2025.v1.txt"

* --- Derived paths (do not edit) ---
global fuzzy_dir  "$root/Fuzzy_match"
global ia_dir     "$root/IA review"
global ia_agent   "$root/IA review/IA_Agent_mode"
global out_dir    "$root/Output"

di "Config loaded. Root: $root"
