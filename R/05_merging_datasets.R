# ============================================================================ #
# 5. Merging Datasets for Further Analysis                                     #
# Author: Felippe Lazar, Universidade de São Paulo, 2024                       #
# ============================================================================ #

# Loading Required Packages
library(httr)
library(jsonlite)
library(listviewer)
library(purrr)
library(data.table)
library(dplyr) 
library(tidyverse)
library(glue)
library(stringi)
library(geosphere)
source('R/aux_functions.R')
library(tidylog)

# ============================================================================ #
# 1. Loading the Locations Initial Database for Analysis      

# Importing Locations
df_loc <- arrow::read_parquet('data/id_trial_locations_contacts.parquet')
included_trials <- read_rds('data/included_trials_ids.rds')

# Filtering by The Trials Found and Selecting the Variables to Posterior Analysis
df_loc <- df_loc %>%
      dplyr::filter(study_nct_id %in% included_trials) %>%
      dplyr::mutate(id_facility_full_address = as.character(id_facility_full_address)) %>%
      dplyr::select(study_nct_id, id_facility_full_address, study_locations.facility, study_locations.zip, study_locations.status, study_locations.country) %>%
      dplyr::distinct(study_nct_id, id_facility_full_address,  .keep_all = TRUE)

# ============================================================================ #
# 2. Adding Google Matched Locations

# Importing Google Outputs
df_matched_locations <- read.table('data/post-processed/final_matched_ids_google.csv', sep = ';') %>%
      dplyr::mutate(id_facility_full_address = as.character(id_facility_full_address))

# Opening the Google to be Used as Mask - Only to retrieve the additional data needed
df_google_output <- bind_rows(arrow::read_parquet('data/google_outputs.parquet'))
df_google_output_long <- do.call(rbind, lapply(0:29, longGoogleLocations, df_google_output))

df_google_mask <- df_google_output_long %>%
      dplyr::select(candidates.place_id, candidates.name, candidates.formatted_address,
                    candidates.geometry.location.lat, candidates.geometry.location.lng) %>%
      dplyr::distinct(candidates.place_id, .keep_all = TRUE)

# ============================================================================ #
# 3. Importing Cancer Conditions

# Loading Conditions Labels
df_cancer_conditions <- readxl::read_excel('data/aux_files/trial_metadata_workbook.xlsx', sheet = 'cancer_categories_unformatted')
df_cancer_conditions <- df_cancer_conditions %>%
      dplyr::select(study_nct_id, new_cancer_category_code)

# ============================================================================ #
# 4. Adding Trial Phase

# Importing Trial Phase
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
# 7. Getting Country Demographics

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

df_merged %>% distinct(study_nct_id) %>% nrow() # 1381
df_merged %>% nrow() #80151
df_merged %>% tidylog::filter(!candidates.place_id %in% c('Generic Site Name', 'Google Unfound Location', 'Manual Review Exclusion')) %>% distinct(candidates.place_id) %>% nrow() # 7107

# Applying the Filter of Non-Tumor
df_merged <- df_merged %>%
      tidylog::filter(new_cancer_category_code != 'NTUMD')

df_merged %>% distinct(study_nct_id) %>% nrow() # 1287
df_merged %>% nrow() #77625
df_merged %>% tidylog::filter(!candidates.place_id %in% c('Generic Site Name', 'Google Unfound Location', 'Manual Review Exclusion')) %>% distinct(candidates.place_id) %>% nrow() # 6634

rio::export(df_merged, 'data/post-processed/df_merged_150126.xlsx')
rio::export(df_merged, 'data/post-processed/df_merged_150126.parquet')

# ============================================================================ #
# 5. Re-matching Google Unfound or Generic Facilities Names or Wrong Matched Names by the Search Address Zip Code

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
# FALSE  TRUE 
# 64018 13607 

table(df_zip_rematched$candidates.place_id %in% c('Generic Site Name', 'Google Unfound Location', 'Manual Review Exclusion'))
# FALSE  TRUE 
# 70827  6798 
