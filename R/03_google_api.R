# ============================================================================ #
# 3. Calling Google API for Trials Locations                                   #
# Author: Felippe Lazar Neto, Universidade de São Paulo, 2024                  #
# ============================================================================ #
#
# Purpose
#   Geocodes every unique site of the included trials with the Google Places API
#   ("Find Place From Text"). For each site, Google returns one or more candidate
#   places (name, formatted address, coordinates, place_id). The place_id is
#   later used to decide which registry entries refer to the same physical
#   facility (script 04).
#
# Steps and numbers (July-August 2024)
#   Unique sites (facility + address) of the 1,381 included trials ...... 36,147
#     -> Generic / masked site names, NOT sent to Google ................. 6,221
#        (e.g. "Research Site", "Novartis Investigative Site"; the facility
#        cannot be identified from these names)
#     -> Sites with a potentially identifiable address ................... 29,926
#          -> without a Google Place ID .................................. 1,940
#             (1,939 with no candidate returned + 1 site without a facility
#              name, absent from data/google_outputs.parquet)
#          -> with at least one Google Place ID ........................... 27,986
#
# Running this script
#   Requires a Google Places API key (a paid service; see 00_aux_functions.R).
#   Google results change over time. To reproduce the manuscript, use the saved
#   output data/google_outputs.parquet and start from script 04.
#
# Input  : data/included_trials_ids.rds            (script 02)
#          data/id_trial_locations_contacts.parquet (script 02)
# Output : data/unknown_names_id_facility_full_address.rds  IDs of generic-name sites
#          data/google_outputs.parquet                      raw Google candidates, wide format
#
# Next   : 04_tidying_locations.R
# ============================================================================ #

# Loading Required Packages
library(tidyverse)
library(glue)
library(stringi)
source('R_public/00_aux_functions.R')

# ============================================================================ #
# 1. Selecting the Sites to Geocode
# Importing Included Trials
included_trials <- readRDS('data/included_trials_ids.rds')

# One row per unique site (facility + address) of the included trials
df_gmaps <- arrow::read_parquet('data/id_trial_locations_contacts.parquet') %>%
      dplyr::filter(study_nct_id %in% included_trials) %>%
      distinct(id_facility_full_address, .keep_all = TRUE)

# Generic or masked site names used by some sponsors. They do not identify a
# facility, so they are not geocoded and are flagged as 'Generic Site Name' in
# script 04. The list was built by manual review of the most frequent names.
unknown_names <- c('Research Site', 'GSK Investigational Site', 'Novartis Investigative Site', 'Clinical Trial Site', 'Arrivent Investigative Site',
  'WK28 Investigative Site', 'ArriVent Investigative Site', 'Allist Investigative Site', 'SB Investigative Site', 'Local Institution',
  'Ferring Investigational Site', 'Exelixis Clinical Site', 'Site Number:', 'Regeneron Study Site', 'Summit Therapeutics Research Center', 'Mg0014', 'GC2202 Study Site',
  'Endo Site', 'Investigational Site Number')

unknown_names_regex <- paste0(unknown_names, collapse = '|')

unknown_names_id_facility_full_address <- df_gmaps %>%
      filter(stri_detect_regex(study_locations.facility, unknown_names_regex)) %>%
      pull(id_facility_full_address)

saveRDS(unknown_names_id_facility_full_address, 'data/unknown_names_id_facility_full_address.rds')

# ============================================================================ #
# 2. Querying Google Places with the Full Address as Registered
# The search text is "Facility, City, State, Zip, Country" (built in script 02),
# without any further editing of the facility name.

google_outputs <- list()
i = 1
max_iter = 99999 # Safety limit; lower it (e.g. to 10) to test the loop before a full run

for(address in df_gmaps %>% split(1:nrow(.))){

      search_address = address$facility_full_address_new
      id_address = as.character(address$id_facility_full_address)

      if(id_address %in% unknown_names_id_facility_full_address) next
      if(i == max_iter) break

      print(glue('Downloading Address Number - It. ID: {i} Address ID: {id_address} - {search_address}'))
      google_outputs[[id_address]] <- getPlacesAPI(search_address)

      Sys.sleep(0.1) # Avoids hitting the API rate limit
      i = i + 1

}

# ============================================================================ #
# 3. Saving the Google Output
# Each response is flattened to one row per site. Multiple candidates become
# repeated column sets (candidates.place_id, candidates.place_id.1, ...), which
# are reshaped to long format with longGoogleLocations() in script 04.
# Responses that cannot be flattened (no candidates) keep only the site ID.
new_google_outputs_dataframe <- lapply(names(google_outputs), function(x) tryCatch(as.data.frame(google_outputs[[x]]) %>% mutate(id_facility_full_address = x), error = function(e) data.frame(id_facility_full_address = x)))
new_google_outputs_dataframe <- new_google_outputs_dataframe %>% bind_rows()

new_google_outputs_dataframe %>% distinct(id_facility_full_address, .keep_all = T) %>% arrow::write_parquet('data/google_outputs.parquet')

# Sites for which Google returned no candidate (1,939; together with the one
# site without a facility name, these are the 1,940 sites without a Google
# Place ID in the flowchart). These are handled in script 04.
missing_addresses <- arrow::read_parquet('data/google_outputs.parquet') %>% filter(is.na(candidates.place_id)) %>% pull(id_facility_full_address)
length(missing_addresses) # 1939
