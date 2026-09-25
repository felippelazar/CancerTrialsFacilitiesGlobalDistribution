# ============================================================================ #
# 4. Tidying Final Locations Information                                       #
# Author: Felippe Lazar Neto, Universidade de São Paulo, 2025                  #
# ============================================================================ #
#
# Purpose
#   Assigns one Google Place ID to every unique site of the included trials and
#   groups Place IDs that refer to the same research facility, producing the
#   list of unique recruiting centres used in the analysis.
#
# Steps and numbers (Flowchart, second panel)
#   Unique sites (facility + address) ................................... 36,147
#     -> Generic facility names ('Generic Site Name') .................... 6,221
#     -> Without a Google Place ID ('Google Unfound Location') ........... 1,940
#     -> Sites with >=1 Google Place ID .................................. 27,986
#          31,217 site x Place ID pairs (3,081 sites with >1 candidate)
#          1,016 pairs judged wrong in manual review -> 30,201 pairs remain
#     -> Top Google candidate judged wrong ('Manual Review Exclusion') ... 780
#     -> Sites with an accepted Google Place ID .......................... 27,206
#          9,355 unique Google Place IDs
#          2,248 Place IDs merged into another Place ID of the same facility
#          7,107 unique Google Place IDs (recruiting centres)
#   (The 94 non-cancer trials are removed later, in script 06.)
#
# Manual review of Google candidates
#   Every site x candidate pair was reviewed by one of four investigators
#   (facility name and address in the registry vs. name and address returned by
#   Google). Pairs marked as wrong are stored in
#   data/manual_review_google_candidates.csv. Only the FIRST (top-ranked) Google
#   candidate of each site is used; if it was marked wrong, the site is labelled
#   'Manual Review Exclusion' and is not re-assigned to a lower-ranked candidate.
#
# Grouping of the same facility (revised analysis)
#   Different Place IDs can refer to the same facility (e.g. "Hospital X" and
#   "Hospital X - Dept. of Haematology"). All pairs of Place IDs located within
#   1,000 m of each other (Haversine distance; 10,974 pairs) were classified by
#   Gemini 3.0 Pro (prompt in data/gemini-pro/prompts/prompt_location_same).
#   Pairs classified as the same facility with HIGH certainty are merged
#   (6,388 pairs, of which 4,072 involve Place IDs present in this dataset).
#   Merged pairs are treated as a network: Place IDs connected directly or
#   indirectly form one facility, labelled with the Place ID that has the most
#   sites. Gemini's classification was validated against manual review on a
#   random sample of 80 Brazilian pairs (script 00_validation_gemini.R).
#
# Input  : data/id_trial_locations_contacts.parquet       (script 02)
#          data/included_trials_ids.rds                   (script 02)
#          data/google_outputs.parquet                    (script 03)
#          data/unknown_names_id_facility_full_address.rds (script 03)
#          data/manual_review_google_candidates.csv       manual review of Google candidates
#          data/classificacao_near_locations_clintrials_v2.xlsx  Place ID pairs within 1,000 m
#          data/gemini-pro/same_location/*_gemini_pro_3.0_1.csv  Gemini 3.0 Pro classification of each pair
# Output : data/membership_df.parquet                     Place ID -> facility Place ID
#          data/post-processed/final_matched_ids_google.csv  site ID -> final facility Place ID
#
# Next   : 05_country_indicators.R
# ============================================================================ #

# Loading Required Packages
library(tidyverse)
library(glue)
library(igraph)
source('R_public/00_aux_functions.R')
library(tidylog)

# ============================================================================ #
# 1. Loading the Unique Sites of the Included Trials
df_loc <- arrow::read_parquet('data/id_trial_locations_contacts.parquet')
included_trials <- read_rds('data/included_trials_ids.rds')

# One row per unique site (36,147)
df_loc <- df_loc %>%
      dplyr::filter(study_nct_id %in% included_trials) %>%
      dplyr::mutate(id_facility_full_address = as.character(id_facility_full_address)) %>%
      dplyr::distinct(id_facility_full_address, .keep_all = TRUE) %>%
      dplyr::select(id_facility_full_address)

# ============================================================================ #
# 2. Adding Google Maps Outputs
# Reshaping the Google candidates to long format: one row per site x candidate
# (up to 30 candidates per site, numbered 0 to 29 in ranking order)
df_google_output <- bind_rows(arrow::read_parquet('data/google_outputs.parquet'))
df_google_output <- df_google_output %>% tidylog::distinct(id_facility_full_address, .keep_all = TRUE)
df_google_output_long <- do.call(rbind, lapply(0:29, longGoogleLocations, df_google_output))

df_google_output_long <- df_google_output_long %>%
      dplyr::filter(!(is.na(candidates.formatted_address)))

# ID of each site x candidate pair ("<site ID>-<candidate number>"), the unit of
# the manual review. 31,217 pairs from 27,986 sites.
df_google_output_long <- df_google_output_long %>%
      tidylog::mutate(id_facility_full_address_candidates_number = paste(id_facility_full_address, candidate_number, sep = '-')) %>%
      tidylog::select(id_facility_full_address, candidate_number, id_facility_full_address_candidates_number, candidates.place_id) %>%
      tidylog::filter(!is.na(candidates.place_id))

# ============================================================================ #
# 3. Sites With Generic Names (not sent to Google in script 03)
unknown_locations_ids <- read_rds('data/unknown_names_id_facility_full_address.rds')

unknown_locations_dataframe <- data.frame(
      id_facility_full_address = as.character(unknown_locations_ids),
      candidate_number = 0,
      candidates.place_id = 'Generic Site Name') %>%
      dplyr::mutate(id_facility_full_address_candidates_number = paste(id_facility_full_address, candidate_number, sep = '-'))

# ============================================================================ #
# 4. Manual Review of the Google Candidates
# manual_review_wrong = 'Yes' -> Google candidate does not correspond to the
# registered facility. 1,016 distinct pairs were judged wrong.
excluded_manual_review <- read_csv('data/manual_review_google_candidates.csv') %>%
      dplyr::filter(manual_review_wrong == 'Yes') %>%
      distinct(id_facility_full_address_candidates_number) %>%
      pull(id_facility_full_address_candidates_number)

# ============================================================================ #
# 5. Assigning One Place ID per Site

# Joining Google candidates (with wrong ones relabelled) and generic-name sites
df_google_and_unknown <- rbind(
      df_google_output_long %>%
            tidylog::mutate(candidates.place_id = ifelse(id_facility_full_address_candidates_number %in% excluded_manual_review, 'Manual Review Exclusion', candidates.place_id)),
      unknown_locations_dataframe
)

# Keeping the top-ranked candidate of each site (candidate 0)
df_joined <- df_loc %>%
      tidylog::left_join(df_google_and_unknown, by = c('id_facility_full_address')) %>%
      dplyr::arrange(candidate_number) %>%
      tidylog::distinct(id_facility_full_address, .keep_all = T)

# Sites without any Google candidate
df_joined <- df_joined %>%
      dplyr::mutate(candidates.place_id = ifelse(is.na(candidates.place_id), 'Google Unfound Location', candidates.place_id))

# Numbers for the flowchart
df_joined %>%
      tidylog::filter(candidates.place_id != 'Manual Review Exclusion') %>% # 780 excluded by manual review
      tidylog::filter(candidates.place_id != 'Google Unfound Location') %>% # 1,940 without a Google Place ID
      tidylog::filter(candidates.place_id != 'Generic Site Name') # 6,221 with generic names -> 27,206 sites remaining

# Number of sites per Place ID. Used to choose the label of merged facilities.
# Note: this table also contains the 3 labels above, so nrow() = 9,358 =
# 9,355 Google Place IDs + 3 labels.
df_google_count <- df_joined %>%
      dplyr::group_by(candidates.place_id) %>%
      summarise(total_candidates.place_id = n()) %>%
      dplyr::ungroup()

# ============================================================================ #
# 6. Grouping Place IDs That Refer to the Same Facility

# Place ID pairs within 1,000 m (row order = row_number used by Gemini)
df_same_location <- readxl::read_excel('data/classificacao_near_locations_clintrials_v2.xlsx')

# Gemini 3.0 Pro classification of each pair (22 batches of up to 500 pairs)
df_gemini_location <- lapply(list.files('data/gemini-pro/same_location', pattern = 'gemini_pro_3.0_1', full.names = T), function(x) read_csv(x)) %>% do.call(rbind, .)

df_gemini_location <- df_gemini_location %>%
      dplyr::mutate(row_number = as.numeric(row_number)) %>%
      dplyr::left_join(df_same_location %>% dplyr::mutate(row_number = row_number()))

# Keeping pairs classified as the same facility with high certainty (6,388)
df_gemini_location <- df_gemini_location %>%
      dplyr::filter(same_facility == 'Y' & certainty_level == 'High') %>%
      dplyr::select(candidates.place_id.x, candidates.place_id.y)

# Keeping pairs in which both Place IDs are assigned to a site in this dataset (4,072)
df_gemini_location <- df_gemini_location %>%
      dplyr::left_join(df_google_count, by = c('candidates.place_id.x' = 'candidates.place_id')) %>%
      dplyr::left_join(df_google_count, by = c('candidates.place_id.y' = 'candidates.place_id')) %>%
      dplyr::filter(!(is.na(total_candidates.place_id.y) | is.na(total_candidates.place_id.x)))

# Each pair is an edge of a graph; each connected component is one facility
# (3,518 Place IDs in 1,270 facilities, i.e. 2,248 Place IDs merged)
dfGraphTidied <- df_gemini_location %>%
      dplyr::rename(from = candidates.place_id.x,
                    to = candidates.place_id.y)

g <- graph_from_data_frame(dfGraphTidied, directed = FALSE)
components <- components(g)

membership_df <- data.frame(
      candidates.place_id.from = names(components$membership),
      candidates.place_id.to = components$membership
)

# The facility is labelled with its Place ID that has the most sites
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

# ============================================================================ #
# 7. Final Table: Site -> Facility Place ID

# Recoding each merged Place ID to its facility label ("old" ~ "new")
membership_labels <- membership_df %>%
      split(1:nrow(.)) %>%
      sapply(function(x) str2lang(glue('"{x$candidates.place_id.from}" ~ "{x$candidates.place_id.to}"')))

df_final <- df_joined %>%
      dplyr::mutate(candidates.place_id = case_match(candidates.place_id, !!!membership_labels, .default = candidates.place_id))

df_final %>%
      dplyr::select(id_facility_full_address, candidates.place_id) %>%
      write.table('data/post-processed/final_matched_ids_google.csv', sep = ';')

# Unique recruiting centres before excluding non-cancer trials: 7,107
df_final %>%
      dplyr::filter(!candidates.place_id %in% c('Generic Site Name', 'Google Unfound Location', 'Manual Review Exclusion')) %>%
      dplyr::distinct(candidates.place_id) %>% nrow()
