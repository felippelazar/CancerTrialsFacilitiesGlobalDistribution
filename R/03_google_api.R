# ============================================================================ #
# 3. Calling Google API for Trials Locations                                   #
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
source('R/aux_functions.R')

# ============================================================================ #
# 1. Calling Google API AND Saving Output
# Importing Included Trials
included_trials <- readRDS('data/included_trials_ids.rds')

# Importing Locations to Further Call Google API
df_gmaps <- arrow::read_parquet('data/id_trial_locations_contacts.parquet') %>%
      dplyr::filter(study_nct_id %in% included_trials) %>%
      distinct(id_facility_full_address, .keep_all = TRUE)

# Filtering Unknown Addresses - Generic Names
unknown_names <- c('Research Site', 'GSK Investigational Site', 'Novartis Investigative Site', 'Clinical Trial Site', 'Arrivent Investigative Site', 
  'WK28 Investigative Site', 'ArriVent Investigative Site', 'Allist Investigative Site', 'SB Investigative Site', 'Local Institution', 
  'Ferring Investigational Site', 'Exelixis Clinical Site', 'Site Number:', 'Regeneron Study Site', 'Summit Therapeutics Research Center', 'Mg0014', 'GC2202 Study Site',
  'Endo Site', 'Investigational Site Number', 'GC2202 Study Site')

unknown_names_regex <- paste0(unknown_names, collapse = '|')

unknown_names_id_facility_full_address <- df_gmaps %>% 
      filter(stri_detect_regex(df_gmaps$study_locations.facility, unknown_names_regex)) %>%
      pull(id_facility_full_address)

saveRDS(unknown_names_id_facility_full_address, 'data/unknown_names_id_facility_full_address.rds')

# ============================================================================ #
# Searching for Addresses Without Any Changes in The Name

google_outputs <- list()
i = 1; max_iter = 99999

for(address in df_gmaps %>% split(1:nrow(.))){
      
      search_address = address$facility_full_address_new
      id_address = as.character(address$id_facility_full_address)
      
      if(id_address %in% unknown_names_id_facility_full_address) next
      if(i == max_iter) break
      
      print(glue('Downloading Address Number - It. ID: {i} Address ID: {id_address} - {search_address}'))
      google_outputs[[id_address]] <- getPlacesAPI(search_address, 
                                                   api_key = 'XXXXXXXXXXXXXXXXXXXXXXXXXX', 
                                                   secret_key = 'XXXXXXXXXXXXXXXXXXXXXXXXXX')

      Sys.sleep(0.1)
      i = i + 1
      
}

# Transforming into DataFrame
new_google_outputs_dataframe <- lapply(names(google_outputs), function(x) tryCatch(as.data.frame(google_outputs[[x]]) %>% mutate(id_facility_full_address = x), error = function(e) data.frame(id_facility_full_address = x)))
new_google_outputs_dataframe <- new_google_outputs_dataframe %>% bind_rows()

# Saving Google Outputs
new_google_outputs_dataframe %>% distinct(id_facility_full_address, .keep_all = T) %>% arrow::write_parquet('data/google_outputs.parquet')

# Getting only Missing Values from First Iteration
missing_addresses <- arrow::read_parquet('data/google_outputs.parquet') %>% filter(is.na(candidates.place_id)) %>% pull(id_facility_full_address)
length(missing_addresses) # 1939
