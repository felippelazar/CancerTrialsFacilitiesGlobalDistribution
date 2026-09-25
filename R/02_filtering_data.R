# ============================================================================ #
# 2. Creating Location IDs and Applying the Trial Selection Criteria           #
# Author: Felippe Lazar Neto, Universidade de São Paulo, 2024                  #
# ============================================================================ #
#
# Purpose
#   (1) Builds a standardised, searchable address for every listed site and
#       numeric IDs for sites and addresses.
#   (2) Applies the eligibility criteria of the study to select trials.
#
# Selection criteria (trial level)
#   Recruiting cancer trials with >=1 listed site ................... 19,523
#     -> Interventional ............................................ 14,862
#     -> Primary purpose Treatment or Supportive Care .............. 12,303
#     -> Phase 3 (includes the 232 trials registered as Phase 2/3) .. 1,381
#
# Input  : data/trial_locations_contacts.parquet   (script 01)
#          data/trial_information.parquet          (script 01)
#          data/trial_phase_design.parquet         (script 01)
# Output : data/id_trial_locations_contacts.parquet  sites with address and IDs
#          data/included_trials_ids.rds              NCT numbers of the 1,381 included trials
#
# Next   : 03_google_api.R
# ============================================================================ #

# Loading Required Packages
library(tidyverse)
library(glue)
library(stringi)

# The numeric IDs below come from as.factor(), whose ordering depends on the
# system collation. The saved IDs were generated with an en_US.UTF-8 locale;
# other locales (e.g. "C") produce different ID numbers.
Sys.setlocale('LC_COLLATE', 'en_US.UTF-8')

# ============================================================================ #
# 1. Loading Locations Data, Creating Unique IDs
df_loc <- arrow::read_parquet('data/trial_locations_contacts.parquet')

# Addresses are built by pasting the registry fields together:
#   facility_full_address_new : "Facility, City, State, Zip, Country" -> text sent to Google (script 03)
#   full_address_new          : "City, State, Zip, Country"
# Missing fields are pasted by glue() as 'NA' and removed by the gsub() steps.
#
# IDs: the address is lower-cased and accents removed, then converted to an
# integer (as.numeric(as.factor())). Identical strings share the same ID, so
#   id_facility_full_address = same facility at the same address, across trials
#   id_full_address          = same city/state/zip/country
#   id_facility              = same facility name
#
# Note: the pattern 'NA[, ]' also removes the letters "NA" when they close an
# upper-case word followed by a space or comma (e.g. "ZNA Middelheim" ->
# "ZMiddelheim"). This affects 13 of the 36,147 unique sites of the included
# trials, only alters the text sent to Google, and is kept as is so that the
# IDs stored in the data/ files remain reproducible.
df_loc <- df_loc %>%
      dplyr::mutate(facility_full_address_new = glue('{study_locations.facility}, {study_locations.city}, {study_locations.state}, {study_locations.zip}, {study_locations.country}')) %>% # Creating Address for Search
      dplyr::mutate(full_address_new = glue('{study_locations.city}, {study_locations.state}, {study_locations.zip}, {study_locations.country}')) %>% # Creating Address for Search
      dplyr::mutate(across(ends_with('address_new'), ~ gsub(' NA$', '', .))) %>%
      dplyr::mutate(across(ends_with('address_new'), ~ gsub('NA[, ]', '', .))) %>%
      dplyr::mutate(across(ends_with('address_new'), ~ gsub('[,] +', ', ', .))) %>%
      dplyr::mutate(across(ends_with('address_new'), ~ trimws(.))) %>%
      dplyr::mutate(id_facility_full_address = tolower(stringi::stri_trans_general(facility_full_address_new, "Latin-ASCII"))) %>% # Remove Accents
      dplyr::mutate(id_full_address = tolower(stringi::stri_trans_general(full_address_new, "Latin-ASCII"))) %>% # Remove Accents
      dplyr::mutate(id_facility = tolower(stringi::stri_trans_general(study_locations.facility, "Latin-ASCII"))) %>% # Remove Accents
      dplyr::mutate(across(c('id_full_address', 'id_facility_full_address', 'id_facility'), ~ as.numeric(as.factor(.))))

# Saving the Locations with the New IDs
df_loc %>%
      arrow::write_parquet('data/id_trial_locations_contacts.parquet')

# ============================================================================ #
# 2. Filtering by Selection Criteria
# Each criterion returns the vector of NCT numbers that satisfy it

# Interventional trials (excludes observational and expanded access)
design_type_criterion <- arrow::read_parquet('data/trial_information.parquet') %>%
      dplyr::select(study_nct_id, study_design_type) %>%
      dplyr::filter(study_design_type == 'INTERVENTIONAL') %>%
      dplyr::pull(study_nct_id)

# Primary purpose: Treatment or Supportive Care
treatment_type_criterion <- arrow::read_parquet('data/trial_information.parquet') %>%
      dplyr::select(study_nct_id, study_design_primary_purpose) %>%
      dplyr::filter(study_design_primary_purpose %in% c('TREATMENT', 'SUPPORTIVE_CARE')) %>%
      dplyr::pull(study_nct_id)

# Phase 3. A Phase 2/3 trial is listed with both PHASE2 and PHASE3 and is
# therefore included (232 of the 1,381 included trials)
phase_trial_criterion <- arrow::read_parquet('data/trial_phase_design.parquet') %>%
      dplyr::filter(study_design_phase %in% c('PHASE3')) %>%
      dplyr::pull(study_nct_id)

# Applying the criteria in sequence (counts are the trials remaining after each step)
included_trials <- df_loc %>%
      tidylog::distinct(study_nct_id, id_facility_full_address) %>% # 191,767 trial-site pairs
      tidylog::distinct(study_nct_id) %>%                          # 19,523 trials
      tidylog::filter(study_nct_id %in% design_type_criterion) %>% # 14,862 interventional trials
      tidylog::filter(study_nct_id %in% treatment_type_criterion) %>% # 12,303 treatment or supportive care trials
      tidylog::filter(study_nct_id %in% phase_trial_criterion) %>% # 1,381 phase 3 trials
      pull(study_nct_id)

write_rds(included_trials, 'data/included_trials_ids.rds')
