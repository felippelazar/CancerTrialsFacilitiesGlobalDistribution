# ============================================================================ #
# 2. Filtering and Tidying Locations Data                                      #
# Author: Felippe Lazar Neto, Universidade de São Paulo, 2024                  #
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

# ============================================================================ #
# 1. Loading Locations Data, Creating Unique ID
df_loc <- arrow::read_parquet('data/trial_locations_contacts.parquet')

# Creating Unique ID by Combining All Strings and Applying Some Parameters
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

# Saving ID Created in a New Identified File
df_loc %>%
      arrow::write_parquet('data/id_trial_locations_contacts.parquet')

# ============================================================================ #
# 2. Filtering by Selection Criteria
# Getting Only Phase II or Phase III Trials
design_type_criterion <- arrow::read_parquet('data/trial_information.parquet') %>%
      dplyr::select(study_nct_id, study_design_type) %>%
      dplyr::filter(study_design_type == 'INTERVENTIONAL') %>%
      dplyr::pull(study_nct_id)

treatment_type_criterion <- arrow::read_parquet('data/trial_information.parquet') %>%
      dplyr::select(study_nct_id, study_design_primary_purpose) %>%
      dplyr::filter(study_design_primary_purpose %in% c('TREATMENT', 'SUPPORTIVE_CARE')) %>%
      dplyr::pull(study_nct_id)

phase_trial_criterion <- arrow::read_parquet('data/trial_phase_design.parquet') %>%
      dplyr::filter(study_design_phase %in% c('PHASE3')) %>%
      dplyr::pull(study_nct_id)

# Filtering Locations for Google Maps API Calling
included_trials <- df_loc %>% # 
      tidylog::distinct(study_nct_id, id_facility_full_address) %>% # Remaining 191,767 Trials Locations
      tidylog::distinct(study_nct_id) %>% # Remaining 19,523 Trials
      tidylog::filter(study_nct_id %in% design_type_criterion) %>% # Remaining 14,862 Intervention Trials - Treatment or Supportive Care Trials
      tidylog::filter(study_nct_id %in% treatment_type_criterion) %>% # 12,303 rows remaining
      tidylog::filter(study_nct_id %in% phase_trial_criterion) %>% # 1,381 Phase Trial Criterion Remaining (Only)
      pull(study_nct_id) 

write_rds(included_trials, 'data/included_trials_ids.rds')

