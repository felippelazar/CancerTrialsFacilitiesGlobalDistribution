# ============================================================================ #
# 0. Auxiliary Functions                                                       #
# Author: Felippe Lazar Neto, Universidade de São Paulo, 2024                  #
# ============================================================================ #

# Converting Google Locations to Long Format
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


# Return the PLACES API from an address
getPlacesAPI <- function(search_address, api_key = 'API_PLACEHOLDER', secret_key = '') {
      
      # Creating Custom Headers
      custom_headers <- c('User-Agent' = 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) Gecko/2009021910 Firefox/3.0.7')
      
      # Creating GET response
      resultPlacesAPI <- 
            GET(
                  url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json",
                  .headers = custom_headers,
                  query = list(
                        fields = 'formatted_address,geometry,name,place_id',
                        input = search_address,
                        inputtype = 'textquery',
                        language = 'en',
                        key = api_key,
                        signature = secret_key
                  )
            )
      
      resultPlacesAPI <- content(resultPlacesAPI, "parsed")
      return(resultPlacesAPI)
      
}
