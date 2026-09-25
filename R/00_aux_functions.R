# ============================================================================ #
# 0. Auxiliary Functions                                                       #
# Author: Felippe Lazar Neto, Universidade de São Paulo, 2024                  #
# ============================================================================ #
#
# Purpose
#   Helper functions shared by the pipeline. This file is sourced by the other
#   scripts (e.g. source('R_public/00_aux_functions.R')) and does not produce
#   any output on its own.
#
# Functions
#   - getPlacesAPI()        : queries the Google Places API ("Find Place") with a
#                             free-text facility address (used in script 03).
#   - longGoogleLocations() : reshapes the wide Google Places output (one column
#                             set per candidate match) into long format, one row
#                             per candidate (used in scripts 04 and 06).
#
# Google API credentials
#   No credentials are stored in this repository. To re-run the geocoding step,
#   set your own key as an environment variable before sourcing, e.g. in
#   ~/.Renviron:
#       GOOGLE_PLACES_API_KEY=your_key
#       GOOGLE_PLACES_SIGNING_SECRET=your_secret   (optional)
# ============================================================================ #

library(dplyr)
library(glue)
library(httr)

# ============================================================================ #
# getPlacesAPI
#   Sends one facility address to the Google Places "Find Place From Text"
#   endpoint and returns the parsed JSON response (a list). The response holds
#   up to N candidate places, each with name, formatted address, coordinates
#   (lat/lng), viewport and Google place_id.
#
#   search_address : character, e.g. "Hospital X, Sao Paulo, SP, 01246-000, Brazil"
#   api_key        : Google Places API key (read from the environment by default)
#   secret_key     : optional URL-signing secret (read from the environment)
getPlacesAPI <- function(search_address,
                         api_key = Sys.getenv('GOOGLE_PLACES_API_KEY'),
                         secret_key = Sys.getenv('GOOGLE_PLACES_SIGNING_SECRET')) {

      if(api_key == '') stop('Google Places API key not found. Set GOOGLE_PLACES_API_KEY (see header of 00_aux_functions.R).')

      # Creating Custom Headers
      custom_headers <- c('User-Agent' = 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) Gecko/2009021910 Firefox/3.0.7')

      # Creating GET response
      resultPlacesAPI <-
            GET(
                  url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json",
                  add_headers(.headers = custom_headers),
                  query = list(
                        fields = 'formatted_address,geometry,name,place_id',
                        input = search_address,
                        inputtype = 'textquery',
                        language = 'en',
                        key = api_key,
                        signature = if(secret_key == '') NULL else secret_key
                  )
            )

      resultPlacesAPI <- content(resultPlacesAPI, "parsed")
      return(resultPlacesAPI)

}

# ============================================================================ #
# longGoogleLocations
#   After flattening the Google responses to a data frame (script 03), each
#   candidate match sits in its own set of columns: the first candidate has no
#   suffix (e.g. candidates.place_id), the second has '.1'
#   (candidates.place_id.1), the third '.2', and so on. This function extracts
#   the columns of ONE candidate and renames them to a common set of names, so
#   that all candidates can be stacked with rbind():
#
#       do.call(rbind, lapply(0:29, longGoogleLocations, df_google_output))
#
#   col_number : candidate index (0 = first candidate, 1 = second, ...)
#   dataframe  : wide Google output (data/google_outputs.parquet)
#   returns    : one row per facility for that candidate, plus 'candidate_number'
longGoogleLocations <- function(col_number, dataframe){

      if(col_number == 0) col_id <- '' else col_id <- glue('.{col_number}')

      new_dataframe <- dataframe %>%
            dplyr::select(starts_with('id'),
                          glue('candidates.formatted_address{col_id}'),
                          glue('candidates.geometry.location.lat{col_id}'),
                          glue('candidates.geometry.location.lng{col_id}'),
                          glue('candidates.geometry.viewport.northeast.lat{col_id}'),
                          glue('candidates.geometry.viewport.northeast.lng{col_id}'),
                          glue('candidates.geometry.viewport.southwest.lat{col_id}'),
                          glue('candidates.geometry.viewport.southwest.lng{col_id}'),
                          glue('candidates.place_id{col_id}'),
                          glue('candidates.name{col_id}')
            ) %>%
            dplyr::rename(
                  'candidates.formatted_address' = glue('candidates.formatted_address{col_id}'),
                  'candidates.geometry.location.lat' = glue('candidates.geometry.location.lat{col_id}'),
                  'candidates.geometry.location.lng' = glue('candidates.geometry.location.lng{col_id}'),
                  'candidates.geometry.viewport.northeast.lat' = glue('candidates.geometry.viewport.northeast.lat{col_id}'),
                  'candidates.geometry.viewport.northeast.lng' = glue('candidates.geometry.viewport.northeast.lng{col_id}'),
                  'candidates.geometry.viewport.southwest.lng' = glue('candidates.geometry.viewport.southwest.lng{col_id}'),
                  'candidates.geometry.viewport.southwest.lat' = glue('candidates.geometry.viewport.southwest.lat{col_id}'),
                  'candidates.name' = glue('candidates.name{col_id}'),
                  'candidates.place_id' = glue('candidates.place_id{col_id}')
            ) %>%
            dplyr::mutate(candidate_number = glue('{col_number}')) %>%
            dplyr::distinct_all()

      return(new_dataframe)

}
