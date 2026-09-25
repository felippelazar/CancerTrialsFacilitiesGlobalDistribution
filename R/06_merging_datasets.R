# ============================================================================ #
# 6. Merging Datasets for Further Analysis                                     #
# Author: Felippe Lazar Neto, Universidade de São Paulo, 2025                  #
# ============================================================================ #
#
# Purpose
#   Builds the analysis datasets. Each row is one trial x site pair ("non-unique
#   location" in the flowchart), with the trial characteristics, the facility
#   (Google Place ID) assigned in script 04 and the country indicators.
#
# Steps and numbers (Flowchart, first panel)
#   Trial x site pairs of the 1,381 included trials ..................... 80,151
#     -> 94 non-cancer trials excluded (e.g. hyperparathyroidism)
#   Trial x site pairs of the 1,287 cancer trials ....................... 77,625
#     MAIN ANALYSIS (df_merged)
#       identified (assigned to a facility) .............................. 64,018
#       unidentified (generic name, not found in Google or top Google
#       candidate rejected in manual review) ............................. 13,607
#       unique facilities ................................................ 6,634
#       (7,107 before excluding non-cancer trials; 473 facilities only
#        took part in non-cancer trials)
#     SENSITIVITY ANALYSIS (df_zip_rematched)
#       unidentified pairs re-matched by ZIP code and country ............ 6,809
#       identified ....................................................... 70,827
#       unidentified ..................................................... 6,798
#
# Input  : data/id_trial_locations_contacts.parquet         (script 02)
#          data/included_trials_ids.rds                     (script 02)
#          data/google_outputs.parquet                      (script 03)
#          data/post-processed/final_matched_ids_google.csv (script 04)
#          data/trial_phase_design.parquet, data/trial_information.parquet (script 01)
#          data/aux_files/trial_metadata_workbook.xlsx      trial classification:
#             cancer category, intervention type and lead sponsor type
#          data/post-processed/trials_demographics.parquet  country indicators (script 05)
# Output : data/post-processed/df_merged_150126.parquet       main analysis
#          data/post-processed/df_merged_150126.xlsx          same, Excel copy
#          data/post-processed/df_zip_rematched_150126.parquet sensitivity analysis
#
# Next   : 07_data_analysis.R (main) and 08_sensitivity_analysis.R
# ============================================================================ #

# Loading Required Packages
library(tidyverse)
library(glue)
source('R_public/00_aux_functions.R')
library(tidylog)

# ============================================================================ #
# 1. Loading the Locations Initial Database for Analysis      

# Importing Locations
df_loc <- arrow::read_parquet('data/id_trial_locations_contacts.parquet')
included_trials <- read_rds('data/included_trials_ids.rds')

# One row per trial x site pair of the 1,381 included trials (80,151)
df_loc <- df_loc %>%
      dplyr::filter(study_nct_id %in% included_trials) %>%
      dplyr::mutate(id_facility_full_address = as.character(id_facility_full_address)) %>%
      dplyr::select(study_nct_id, id_facility_full_address, study_locations.facility, study_locations.zip, study_locations.status, study_locations.country) %>%
      dplyr::distinct(study_nct_id, id_facility_full_address,  .keep_all = TRUE)

# ============================================================================ #
# 2. Adding Google Matched Locations

# Site ID -> final facility Place ID (script 04)
df_matched_locations <- read.table('data/post-processed/final_matched_ids_google.csv', sep = ';') %>%
      dplyr::mutate(id_facility_full_address = as.character(id_facility_full_address))

# Name, address and coordinates of each Place ID (from the Google output)
df_google_output <- bind_rows(arrow::read_parquet('data/google_outputs.parquet'))
df_google_output_long <- do.call(rbind, lapply(0:29, longGoogleLocations, df_google_output))

df_google_mask <- df_google_output_long %>%
      dplyr::select(candidates.place_id, candidates.name, candidates.formatted_address,
                    candidates.geometry.location.lat, candidates.geometry.location.lng) %>%
      dplyr::distinct(candidates.place_id, .keep_all = TRUE)

# ============================================================================ #
# 3. Importing Cancer Conditions

# Cancer category of each trial. Code 'NTUMD' = non-cancer condition
# (94 trials), excluded in Section 9
df_cancer_conditions <- readxl::read_excel('data/aux_files/trial_metadata_workbook.xlsx', sheet = 'cancer_categories_unformatted')
df_cancer_conditions <- df_cancer_conditions %>%
      dplyr::select(study_nct_id, new_cancer_category_code)

# ============================================================================ #
# 4. Adding Trial Phase

# One indicator column per phase (design_phase2, design_phase3, ...); Phase 2/3
# trials have both set to 1
df_design <- arrow::read_parquet('data/trial_phase_design.parquet')

df_design <- df_design %>%
      dplyr::distinct_all() %>%
      dplyr::mutate(study_design_phase_value = 1) %>%
      tidyr::pivot_wider(names_from = study_design_phase, values_from = study_design_phase_value) %>%
      dplyr::mutate(across(everything(), ~ replace_na(., 0))) %>%
      janitor::clean_names() %>%
      setNames(., paste0('design_', names(.))) %>%
      dplyr::rename(study_nct_id = design_study_nct_id)

# ============================================================================ #
# 5. Adding Trial Information

# Importing Data
df_trial_info <- arrow::read_parquet('data/trial_information.parquet')
df_trial_info <- df_trial_info %>%
      dplyr::select(study_nct_id, study_official_title, study_design_primary_purpose, study_lead_sponsor_name, study_design_type, study_status, study_responsible_party)

# ============================================================================ #
# 6. Adding Treatment Information

df_cancer_treatments <- readxl::read_excel('data/aux_files/trial_metadata_workbook.xlsx', sheet = 'cancer_treatment_type_unformatt')
df_cancer_treatments <- df_cancer_treatments %>%
      dplyr::select(study_nct_id, new_intervention_type)

# ============================================================================ #
# 7. Adding Sponsor Information

df_sponsor <- readxl::read_excel('data/aux_files/trial_metadata_workbook.xlsx', sheet = 'research_sponsor_unformatted')
df_sponsor <- df_sponsor %>%
      dplyr::select(study_nct_id, new_study_lead_sponsor_type)

# ============================================================================ #
# 8. Getting Country Demographics

# Country-level indicators (World Bank) joined by country
df_demographics <- arrow::read_parquet('data/post-processed/trials_demographics.parquet')

# ============================================================================ #
# 9. Merging All Information Together

# Joining All Together
df_merged <- df_loc %>%
      tidylog::left_join(df_trial_info) %>%
      tidylog::left_join(df_matched_locations) %>%
      tidylog::left_join(df_google_mask) %>%
      tidylog::left_join(df_cancer_conditions) %>%
      tidylog::left_join(df_cancer_treatments) %>%
      tidylog::left_join(df_sponsor) %>%
      tidylog::left_join(df_design) %>%
      tidylog::left_join(df_demographics)

df_merged %>% distinct(study_nct_id) %>% nrow() # 1,381 trials
df_merged %>% nrow() # 80,151 trial x site pairs
df_merged %>% tidylog::filter(!candidates.place_id %in% c('Generic Site Name', 'Google Unfound Location', 'Manual Review Exclusion')) %>% distinct(candidates.place_id) %>% nrow() # 7,107 facilities

# Excluding the 94 non-cancer trials
df_merged <- df_merged %>%
      tidylog::filter(new_cancer_category_code != 'NTUMD')

df_merged %>% distinct(study_nct_id) %>% nrow() # 1,287 trials
df_merged %>% nrow() # 77,625 trial x site pairs
df_merged %>% tidylog::filter(!candidates.place_id %in% c('Generic Site Name', 'Google Unfound Location', 'Manual Review Exclusion')) %>% distinct(candidates.place_id) %>% nrow() # 6,634 facilities

rio::export(df_merged, 'data/post-processed/df_merged_150126.xlsx')
rio::export(df_merged, 'data/post-processed/df_merged_150126.parquet')

# ============================================================================ #
# 10. Sensitivity Analysis: Re-matching Unidentified Sites by ZIP Code
# If a ZIP code + country contains exactly ONE identified facility, the
# unidentified sites registered with that ZIP code + country are assigned to it.
# ZIP codes are compared after removing non-alphanumeric characters; missing or
# placeholder ZIP codes ("0", "000000") are not used.

# ZIP codes + country with a single identified facility
df_zip_places_id <- df_merged %>%
      dplyr::select(study_locations.zip, study_locations.country, candidates.place_id, candidates.name) %>%
      tidylog::filter(!(is.na(study_locations.zip) | study_locations.zip %in% c("0", "000000"))) %>%
      dplyr::mutate(study_locations.zip = gsub('[^[:alnum:]]+', '', study_locations.zip)) %>%
      tidylog::filter(!candidates.place_id %in% c('Generic Site Name', 'Google Unfound Location', 'Manual Review Exclusion')) %>%
      dplyr::distinct(study_locations.zip, study_locations.country, candidates.place_id, .keep_all = TRUE) %>%
      group_by(study_locations.zip, study_locations.country) %>%
      dplyr::mutate(n_same_zip = n()) %>%
      ungroup() %>%
      dplyr::filter(n_same_zip == 1) %>%
      dplyr::select(-n_same_zip)

# Unidentified pairs re-matched when possible, then bound to the identified ones
df_zip_rematched <- df_merged %>%
      tidylog::filter(candidates.place_id %in% c('Generic Site Name', 'Google Unfound Location', 'Manual Review Exclusion')) %>%
      mutate(study_locations.zip = gsub('[^[:alnum:]]+', '', study_locations.zip)) %>%
      left_join(df_zip_places_id, 
                by = c("study_locations.zip", "study_locations.country"), 
                suffix = c("", "_new")) %>%
      mutate(candidates.place_id = ifelse(!is.na(candidates.place_id_new), candidates.place_id_new, candidates.place_id)) %>%
      mutate(candidates.name = ifelse(!is.na(candidates.name_new), candidates.name_new, candidates.name)) %>%
      select(-candidates.place_id_new, -candidates.name_new) %>% 
      dplyr::bind_rows(df_merged %>%
      dplyr::filter(!candidates.place_id %in% c('Generic Site Name', 'Google Unfound Location', 'Manual Review Exclusion')))

arrow::write_parquet(df_zip_rematched, 'data/post-processed/df_zip_rematched_150126.parquet')

table(df_merged$candidates.place_id %in% c('Generic Site Name', 'Google Unfound Location', 'Manual Review Exclusion'))
# Main analysis: identified (FALSE) and unidentified (TRUE) pairs
# FALSE  TRUE 
# 64018 13607 

table(df_zip_rematched$candidates.place_id %in% c('Generic Site Name', 'Google Unfound Location', 'Manual Review Exclusion'))
# Sensitivity analysis: 13,607 - 6,798 = 6,809 pairs re-matched
# FALSE  TRUE 
# 70827  6798
