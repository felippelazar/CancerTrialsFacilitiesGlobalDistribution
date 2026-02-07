# ============================================================================ #
# 1. Download Clinical Trials Database and Saving Files                        #
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

# ============================================================================ #
# 1. Creating Function to Help Parse JSON (from ClinicalTrialsAPI)

default_value <- function(value) {if (is.null(value)) NA else value}

tidyClinTrialsStudy <- function(n_study, data){
  
  cat('  - Pulling Study: ', n_study, '\n', sep = '')
  
  studyInfo <- list(
    'study_nct_id' = data$studies$protocolSection$identificationModule$nctId[[n_study]],
    'study_acronym' = data$studies$protocolSection$identificationModule$acronym[[n_study]],
    'study_brief_title' = data$studies$protocolSection$identificationModule$briefTitle[[n_study]],
    'study_official_title' = data$studies$protocolSection$identificationModule$officialTitle[[n_study]],
    'study_eligibility_criteria' = data$studies$protocolSection$eligibilityModule$eligibilityCriteria[[n_study]],
    'study_locations' = data$studies$protocolSection$contactsLocationsModule$locations[[n_study]],
    'study_status' = data$studies$protocolSection$statusModule$overallStatus[[n_study]],
    'study_status_verified_date' = data$studies$protocolSection$statusModule$statusVerifiedDate[[n_study]],
    'study_brief_summary' = data$studies$protocolSection$descriptionModule$briefSummary[[n_study]],
    'study_conditions' = t(default_value(data$studies$protocolSection$conditionsModule$conditions[[n_study]])),
    'study_design_phase' = t(default_value(data$studies$protocolSection$designModule$phases[[n_study]])),
    'study_design_primary_purpose' = data$studies$protocolSection$designModule$designInfo$primaryPurpose[[n_study]],
    'study_design_type' = data$studies$protocolSection$designModule$studyType[[n_study]],
    'study_lead_sponsor_name' = data$studies$protocolSection$sponsorCollaboratorsModule$leadSponsor$name[[n_study]],
    'study_lead_sponsor_type' = data$studies$protocolSection$sponsorCollaboratorsModule$leadSponsor$type[[n_study]],
    'study_responsible_party' = data$studies$protocolSection$sponsorCollaboratorsModule$responsibleParty$type[[n_study]],
    'study_arms_interventions_label' = t(default_value(data$studies$protocolSection$armsInterventionsModule$armGroups[[n_study]]$label)),
    'study_arms_interventions_names' = t(default_value(data$studies$protocolSection$armsInterventionsModule$armGroups[[n_study]]$interventionNames)),
    'study_arms_interventions_types' = t(default_value(data$studies$protocolSection$armsInterventionsModule$armGroups[[n_study]]$type))
    )
  
  studyInfo <- lapply(studyInfo, function(x) {if(is.null(x)) return('N/A') else return(x)})
  
  return(studyInfo)
  
}

tidyClinTrialsAPI <- function(data){
  
  n_articles <- length(data$studies$protocolSection$identificationModule$nctId)
  content_list <- lapply(1:n_articles, tidyClinTrialsStudy, data)
  dataframes_list <- lapply(1:length(content_list), function(x) {return(as.data.frame(content_list[[x]]))})
  return(do.call(bind_rows, dataframes_list))
  
}

getTreatmentInfo <- function(col_number, dataframe){
      
      if(col_number == 0) col_id <- '' else col_id <- glue('.{col_number}')
      
      new_dataframe <- dataframe %>%
            dplyr::select(study_nct_id, 
                          glue('study_arms_interventions_types{col_id}'), 
                          glue('study_arms_interventions_names{col_id}'), 
                          glue('study_arms_interventions_label{col_id}')
            ) %>%
            dplyr::rename(
                  'study_arms_interventions_types' = glue('study_arms_interventions_types{col_id}'),
                  'study_arms_interventions_names' = glue('study_arms_interventions_names{col_id}'),
                  'study_arms_interventions_label' = glue('study_arms_interventions_label{col_id}')
            ) %>%
            dplyr::mutate(study_arms_intervention_number = glue('{col_number}')) %>%
            dplyr::distinct_all()
      
      
      return(new_dataframe)
      
}

# ============================================================================ #
# 2. Downloading from ClinicalTrialsAPI

# Setting the Base URL for Downloading
base_url <- "https://clinicaltrials.gov/api/v2/studies"

# Creating Custom Headers
custom_headers <- c('User-Agent' = 'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.7) Gecko/2009021910 Firefox/3.0.7')

# Creating Filter (Cancer AND Actively Recruiting Trials)
query_search <- list(
  filter.overallStatus = 'RECRUITING',
  format = 'json',
  query.cond = 'cancer'
)

# Downloading Clinical Trials Information from API - 23th July 2024
# Initiating the For Looping
search_list <- list()
search_number <- 0
boolPageToken <- TRUE

while(boolPageToken){
      
      search_number = search_number + 1
      cat('Pulling Batch: ', search_number, '\n')
      if(search_number != 1){query_search[['pageToken']] <- data$nextPageToken}
      
      # Creating GET response
      response <- GET(
            url = base_url,
            query = query_search,
            add_headers(.headers = custom_headers)
      )
      
      content <- content(response, "text", encoding = "UTF-8")
      data <- fromJSON(content)
      search_list[[search_number]] <- tidyClinTrialsAPI(data)
      boolPageToken <- !is.null(data$nextPageToken)
      
}

# ============================================================================ #
# 3. Tidying the Dataset Downloaded and Separating It By Different Parts of Information
# The Unit of Interest is the NCT number

# Transforming All in One DataFrame
df_all <- do.call(bind_rows, search_list)
str(df_all)

# Exporting Trial Information
df_all %>%
      dplyr::select(study_nct_id, study_acronym, study_official_title, study_brief_title, study_brief_summary, study_eligibility_criteria,
             study_status, study_status_verified_date, study_design_primary_purpose, study_design_type, 
             study_lead_sponsor_name, study_lead_sponsor_type, study_responsible_party) %>%
      dplyr::distinct_all() %>%
      arrow::write_parquet('data/trial_information.parquet')

# Exporting Tumor Conditions
df_all %>% 
      dplyr::select(study_nct_id, starts_with('study_conditions')) %>%
      tidyr::pivot_longer(cols = starts_with('study_conditions'), values_to = 'study_condition', names_to = NULL) %>%
      dplyr::filter(!is.na(study_condition)) %>%
      dplyr::distinct_all() %>%
      arrow::write_parquet('data/trial_conditions.parquet')

# Exporting Phase Design
df_all %>%
      dplyr::select(study_nct_id, starts_with('study_design_phase')) %>%
      tidyr::pivot_longer(cols = starts_with('study_design_phase'), values_to = 'study_design_phase', names_to = NULL) %>%
      dplyr::filter(!is.na(study_design_phase)) %>%
      dplyr::distinct_all() %>%
      arrow::write_parquet('data/trial_phase_design.parquet')

# Exporting Locations
df_all %>%
      dplyr::select(study_nct_id, starts_with('study_locations')) %>%
      unnest(cols = c(study_locations.contacts, study_locations.geoPoint)) %>%
      distinct_all() %>%
      arrow::write_parquet('data/trial_locations_contacts.parquet')

# Exporting Interventions
df_arms <- df_all %>%
      select(study_nct_id, starts_with('study_arms_interventions'))

list_treatment_arms <- lapply(0:27, getTreatmentInfo, df_arms)
df_arms_tidy <- do.call(rbind, list_treatment_arms)

df_arms_tidy %>%
      unnest(cols = c(study_arms_interventions_names)) %>%
      distinct_all() %>%
      arrow::write_parquet('data/trial_arms_interventions.parquet')


