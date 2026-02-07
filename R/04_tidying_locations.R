# ============================================================================ #
# 4. Tidying Final Locations Information                                       #
# Author: Felippe Lazar, Universidade de São Paulo, 2025                       #
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
#  Loading the Core Library of Locations and Trials
df_loc <- arrow::read_parquet('data/id_trial_locations_contacts.parquet')
included_trials <- read_rds('data/included_trials_ids.rds')

# Filtering by The Trials Found
df_loc <- df_loc %>%
      dplyr::filter(study_nct_id %in% included_trials) %>%
      dplyr::mutate(id_facility_full_address = as.character(id_facility_full_address)) %>%
      dplyr::distinct(id_facility_full_address, .keep_all = TRUE) %>%
      dplyr::select(id_facility_full_address)

# ============================================================================ #
# Adding Google Maps Outputs 

df_google_output <- bind_rows(arrow::read_parquet('data/google_outputs.parquet'))
df_google_output <- df_google_output %>% tidylog::distinct(id_facility_full_address, .keep_all = TRUE)
df_google_output_long <- do.call(rbind, lapply(0:29, longGoogleLocations, df_google_output))

df_google_output_long <- df_google_output_long %>%
      dplyr::filter(!(is.na(candidates.formatted_address)))

# Creating ID of the Manual Review
df_google_output_long <- df_google_output_long %>%
      tidylog::mutate(id_facility_full_address_candidates_number = paste(id_facility_full_address, candidate_number, sep = '-')) %>%
      tidylog::select(id_facility_full_address, candidate_number, id_facility_full_address_candidates_number, candidates.place_id) %>%
      tidylog::filter(!is.na(candidates.place_id))

# ============================================================================ #
# Loading Locations Where it Was Impossible to Find in Google
unknown_locations_ids <- read_rds('data/unknown_names_id_facility_full_address.rds')

unknown_locations_dataframe <- data.frame(
      id_facility_full_address = as.character(unknown_locations_ids),
      candidate_number = 0,
      candidates.place_id = 'Generic Site Name') %>%
      dplyr::mutate(id_facility_full_address_candidates_number = paste(id_facility_full_address, candidate_number, sep = '-'))

# ============================================================================ #
# Adding Manual Review of Locations
# Table with all the Pairs of Locations which were Manually Reviewed and Marked as Wrong
excluded_manual_review <- arrow::read_parquet('manual_excluded_locations.parquet') %>% 
      dplyr::filter(manual_review_wrong == 'Yes') %>% 
      distinct(id_facility_full_address_candidates_number) %>%
      pull(id_facility_full_address_candidates_number)

# ============================================================================ #
# Making the Filter for the Final Locations

# Joining Google and Unknown 
df_google_and_unknown <- rbind(
      df_google_output_long %>% 
            tidylog::mutate(candidates.place_id = ifelse(id_facility_full_address_candidates_number %in% excluded_manual_review, 'Manual Review Exclusion', candidates.place_id)),
      unknown_locations_dataframe
)

df_joined <- df_loc %>%
      tidylog::left_join(df_google_and_unknown, by = c('id_facility_full_address')) %>%
      dplyr::arrange(candidate_number) %>%
      tidylog::distinct(id_facility_full_address, .keep_all = T)

df_joined <- df_joined %>%
      dplyr::mutate(candidates.place_id = ifelse(is.na(candidates.place_id), 'Google Unfound Location', candidates.place_id))

# ============================================================================ #
# Setting Filters to Make the Fluxogram

df_joined %>%
      tidylog::distinct(id_facility_full_address, .keep_all = TRUE) %>% # distinct: no rows removed
      tidylog::filter(candidates.place_id != 'Manual Review Exclusion') %>% # 780 Excluded by Manual Review
      tidylog::filter(candidates.place_id != 'Google Unfound Location') %>% # 1940 Not Found in Google
      tidylog::filter(candidates.place_id != 'Generic Site Name') # 6221 with Generic Names

df_google_count <- df_joined %>%
      dplyr::group_by(candidates.place_id) %>%
      summarise(total_candidates.place_id = n()) %>%
      dplyr::ungroup()

df_google_count %>% nrow()

# ============================================================================ #
# Same Location Analysis

# File with the Groupping of Locations from the Same Facility (Distance Within 1000m)
df_same_location <- readxl::read_excel('data/same_locations_final.xlsx')

# Finding the corrected locations
library(igraph)

dfGraphTidied <- df_same_location %>%
      dplyr::rename(from = candidates.place_id.x,
                    to = candidates.place_id.y)

g <- graph_from_data_frame(dfGraphTidied, directed = FALSE)
components <- components(g)

membership_df <- data.frame(
      candidates.place_id.from = names(components$membership),
      candidates.place_id.to = components$membership
)

# Finding the Higher Number for Each Membership
groupLabels <- membership_df %>%
      dplyr::left_join(df_google_count, by = c('candidates.place_id.from' = 'candidates.place_id')) %>%
      dplyr::arrange(desc(total_candidates.place_id)) %>%
      dplyr::distinct(candidates.place_id.to, .keep_all = TRUE) %>%
      dplyr::select(candidates.place_id.from, candidates.place_id.to) %>%
      dplyr::rename(candidates.place_id.to_label = candidates.place_id.from)

membership_df <- membership_df %>%
      dplyr::left_join(groupLabels, by = c('candidates.place_id.to' = 'candidates.place_id.to')) %>%
      dplyr::select(candidates.place_id.from, candidates.place_id.to_label) %>%
      dplyr::rename(candidates.place_id.to = candidates.place_id.to_label)

rio::export(membership_df, 'data/membership_df.parquet')

# Checking if One Only is Present
membership_labels <- membership_df %>% 
      split(1:nrow(.)) %>%
      sapply(function(x) str2lang(glue('"{x$candidates.place_id.from}" ~ "{x$candidates.place_id.to}"')))

# Now re-creating the final table with all the matched locations
df_final <- df_joined %>%
      dplyr::mutate(candidates.place_id = case_match(candidates.place_id, !!!membership_labels, .default = candidates.place_id))

df_final %>%
      dplyr::select(id_facility_full_address, candidates.place_id) %>%
      write.table('data/post-processed/final_matched_ids_google.csv', sep = ';')


