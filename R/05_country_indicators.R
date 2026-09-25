# ============================================================================ #
# 5. Country Indicators (World Bank)                                           #
# Author: Felippe Lazar Neto, Universidade de São Paulo, 2025                  #
# ============================================================================ #
#
# Purpose
#   Downloads country-level indicators from the World Bank API (income level,
#   region, population, GDP, health and research expenditure, health workforce)
#   for:
#     (1) the countries with at least one site in the included trials, and
#     (2) all 193 UN member states, used in script 07 to add countries with no
#         site as zeros in the country-level analysis.
#
# Indicator values
#   For each indicator and country, the MOST RECENT non-missing year is kept
#   (the year is stored in the '<indicator>_date' columns). Some indicators are
#   only available for older years in some countries (e.g. hospital beds).
#
# Data snapshot
#   (1) downloaded in September 2025; (2) downloaded in September 2026. The
#   World Bank updates its data regularly, so re-running this script returns
#   different values. To reproduce the manuscript, use the saved files.
#
# Input  : World Bank API v2 (https://api.worldbank.org/v2)
# Output : data/post-processed/trials_demographics.parquet            (1) countries with trial sites
#          data/post-processed/all_countries_trials_demographics.parquet (2) all UN member states
#
# Next   : 06_merging_datasets.R
# ============================================================================ #

# Loading Required Packages
library(tidyverse)
library(httr)
library(glue)
library(countrycode)

# ============================================================================ #
# 1. Countries With Trial Sites and Their UN Geographic Region
# Country names as registered in ClinicalTrials.gov

un_regions_country <- list(
      "Europe" = c(
            "France", "Austria", "Belgium", "Finland", "Germany", "Hungary",
            "Italy", "Poland", "Portugal", "Spain", "United Kingdom", "Netherlands",
            "Sweden", "Czechia", "Ireland", "Switzerland", "Greece",
            "Denmark", "Norway", "Romania", "Belarus", "Croatia", "Slovakia",
            "Slovenia", "Bulgaria", "Serbia", "Lithuania", "Ukraine", "Estonia",
            "Latvia", "Iceland", "Bosnia and Herzegovina", "Moldova, Republic of",
            "North Macedonia", "Monaco", "Luxembourg", "Russian Federation"),
      "North America" = c(
            "United States", "Canada", "Gam"),
      "Latin America" = c(
            "Argentina", "Brazil", "Chile", "Colombia", "Costa Rica", "Guatemala",
            "Mexico", "Panama", "Dominican Republic", "Peru", "Martinique", "Puerto Rico"),
      "Africa" = c(
            "Côte D'Ivoire", "Morocco", "Nigeria", "Kenya", "Uganda", "South Africa",
            "Egypt", "Tunisia", "Algeria"),
      "Asia" = c(
            "China", "Hong Kong", "India", "Israel", "Korea, Republic of",
            "Singapore", "Taiwan", "Thailand", "Japan", "Malaysia", "Saudi Arabia",
            "Indonesia", "Philippines", "Vietnam", "Georgia", "Kuwait", "Jordan",
            "Oman", "United Arab Emirates", "Lebanon", "Iran, Islamic Republic of",
            "Syrian Arab Republic", "Armenia", "Cyprus", "Turkey"),
      "Oceania" = c(
            "Australia", "New Zealand")
)

# Recoding rules "country" ~ "region", used with case_match()
un_regions_rename <- un_regions_country %>% stack() %>%
      dplyr::rename(country = values, region_un = ind) %>%
      split(1:nrow(.)) %>%
      sapply(function(x) str2lang(glue('"{x$country}" ~ "{x$region_un}"')))

# ISO codes used to match World Bank records
countries_included <- un_regions_country %>% stack() %>% dplyr::rename(country = values, region_un = ind)
countries_code_iso2c <- countrycode(countries_included$country, origin = 'country.name', destination = 'iso2c')
countries_code_iso3c <- countrycode(countries_included$country, origin = 'country.name', destination = 'iso3c')

# ============================================================================ #
# 2. Downloading From the World Bank API

# Income level and region of every World Bank economy (6 pages of results)
wb_development_data <- lapply(1:6, function(x) {
      res <- GET(glue("https://api.worldbank.org/v2/country?format=json&page={x}"))
      content(res, "parsed")[[2]]  # Extract the data part (second element)
})

wb_development_level_all <- bind_rows(wb_development_data) %>%
      unnest(c(region, adminregion, incomeLevel, lendingType)) %>%
      dplyr::select(
            countryiso3code = id,
            wdi_income_level = incomeLevel,
            wdi_region = region
      ) %>%
      dplyr::group_by(countryiso3code) %>%
      dplyr::slice_tail(n = 1) %>% # Keeps the label (e.g. "High income") of the unnested fields
      dplyr::ungroup()

# World Development Indicators used in the analysis
indicators <- c(
      "wdi_research_expenditure" = "GB.XPD.RSDV.GD.ZS",    # R&D expenditure (% of GDP)
      "wdi_total_population" = "SP.POP.TOTL",              # Total population
      "wdi_health_expenditure_perc" = "SH.XPD.CHEX.GD.ZS", # Current health expenditure (% of GDP)
      "wdi_gdp" = "NY.GDP.MKTP.CD",                        # GDP (current US$)
      "wdi_gdp_per_capita" = "NY.GDP.PCAP.CD",             # GDP per capita (current US$)
      "wdi_human_capital_index" = "HD.HCI.OVRL",           # Human Capital Index
      "wdi_oop_health_expenditure" = "SH.XPD.OOPC.CH.ZS",  # Out-of-pocket expenditure (% of health expenditure)
      "wdi_physician_per_1000" = "SH.MED.PHYS.ZS",         # Physicians per 1,000 people
      "wdi_nurses_per_1000" = "SH.MED.NUMW.P3",            # Nurses and midwives per 1,000 people
      "wdi_hospital_beds_per_1000" = "SH.MED.BEDS.ZS"      # Hospital beds per 1,000 people
)

# Recoding rules "code" ~ "label"
indicators_rename_label <- indicators %>% stack() %>%
      dplyr::rename(indicator_codes = ind, indicator_label = values) %>%
      split(1:nrow(.)) %>%
      sapply(function(x) str2lang(glue('"{x$indicator_label}" ~ "{x$indicator_code}"')))

# All years for all countries, one request per indicator
wb_development_index <- lapply(indicators, function(x) {
      res <- GET(glue("https://api.worldbank.org/v2/en/country/all/indicator/{x}?format=json&date=:&per_page=32500&page=1"))
      content(res, "parsed")[[2]]  # Extract the data part (second element)
})

# Most recent non-missing value of each indicator per country, in wide format
# (<indicator>_value and <indicator>_date columns)
latestIndicators <- function(df_index, country_codes = NULL){

      if(!is.null(country_codes)) df_index <- df_index %>% dplyr::filter(countryiso3code %in% country_codes)

      df_index %>%
            dplyr::filter(indicator %in% indicators) %>%
            dplyr::filter(!is.na(value)) %>%
            arrange(desc(date)) %>%
            dplyr::distinct(indicator, countryiso3code, .keep_all = TRUE) %>%
            dplyr::mutate(indicator = case_match(indicator, !!!indicators_rename_label, .default = indicator)) %>%
            tidyr::pivot_wider(names_from = indicator,
                               values_from = c(value, date),
                               id_cols = countryiso3code,
                               names_glue = "{indicator}_{.value}")
}

wb_development_index_all <- bind_rows(wb_development_index) %>% unnest(c(indicator, country))

# ============================================================================ #
# 3. Output (1): Countries With Trial Sites

wb_development_level_df <- wb_development_level_all %>%
      dplyr::filter(countryiso3code %in% c(countries_code_iso3c, countries_code_iso2c))

wb_development_index_df <- latestIndicators(wb_development_index_all, c(countries_code_iso3c, countries_code_iso2c))

df_descriptive <- cbind(countries_included, countries_code_iso2c, countries_code_iso3c) %>% as.data.frame() %>%
      dplyr::rename(study_locations.country = country) %>%
      dplyr::left_join(wb_development_level_df, by = c('countries_code_iso3c' = "countryiso3code")) %>%
      dplyr::left_join(wb_development_index_df, by = c('countries_code_iso3c' = "countryiso3code")) %>%
      dplyr::mutate(study_locations.country_region = case_match(study_locations.country, !!!un_regions_rename, .default = NA_character_))

df_descriptive %>%
      arrow::write_parquet('data/post-processed/trials_demographics.parquet')

# ============================================================================ #
# 4. Output (2): All UN Member States

# World Bank economies that are not UN member states
non_sovereign <- c(
      "ABW", # Aruba (Netherlands)
      "CUW", # Curaçao (Netherlands)
      "SXM", # Sint Maarten (Netherlands)
      "MAF", # Saint Martin (France)
      "PYF", # French Polynesia (France)
      "NCL", # New Caledonia (France)
      "ASM", # American Samoa (US)
      "GUM", # Guam (US)
      "MNP", # Northern Mariana Islands (US)
      "PRI", # Puerto Rico (US)
      "VIR", # US Virgin Islands (US)
      "BMU", # Bermuda (UK)
      "CYM", # Cayman Islands (UK)
      "GIB", # Gibraltar (UK)
      "TCA", # Turks and Caicos Islands (UK)
      "VGB", # British Virgin Islands (UK)
      "IMN", # Isle of Man (UK)
      "CHI", # Channel Islands (UK)
      "FRO", # Faroe Islands (Denmark)
      "GRL", # Greenland (Denmark)
      "HKG", # Hong Kong SAR (China)
      "MAC", # Macao SAR (China)
      "PSE", # West Bank and Gaza (UN observer)
      "XKX"  # Kosovo (partial recognition)
)

wb_development_level_un <- wb_development_level_all %>%
      dplyr::filter(wdi_income_level != "Aggregates") %>% # Removes regional/income aggregates
      dplyr::filter(!countryiso3code %in% non_sovereign)

stopifnot(nrow(wb_development_level_un) == 193)

wb_development_level_un %>%
      dplyr::left_join(latestIndicators(wb_development_index_all)) %>%
      dplyr::mutate(countryiso3code = tolower(countryiso3code)) %>%
      arrow::write_parquet('data/post-processed/all_countries_trials_demographics.parquet')
