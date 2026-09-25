# ============================================================================ #
# 7. Main Analysis: Tables, Models and Figures                                 #
# Author: Felippe Lazar Neto, Universidade de São Paulo, 2025                  #
# ============================================================================ #
#
# Purpose
#   All analyses of the article on the main dataset (1,287 trials; 77,625
#   trial x site pairs; 6,634 facilities), at three levels:
#     1. Trials     : descriptive tables
#     2. Facilities : descriptive tables, comparison across World Bank regions
#     3. Countries  : trials vs facilities (linear models), correlation with
#                     World Bank indicators, top-20 countries and maps
#     4. Unidentified sites: mixed logistic regression and map
#
# Input  : data/post-processed/df_merged_150126.parquet                (script 06)
#          data/post-processed/all_countries_trials_demographics.parquet (script 05)
# Output : results/table_article_main.xlsx   descriptive tables (one sheet each)
#          plots/main_*.pdf                  figures
#
# Next   : 08_sensitivity_analysis.R
# ============================================================================ #

source('R_public/00_analysis_functions.R')

df <- arrow::read_parquet('data/post-processed/df_merged_150126.parquet') %>% fixRegions()

wb <- createWorkbook()

# ============================================================================ #
# 1. Trial Level

df_trials <- summariseTrials(df)

# All trials
wb <- addStyledSheet(wb, 'trials_summary', tableOneText(
      df_trials %>% dplyr::select(trial_locations_number, trial_locations_number_cat, trial_locations_status_recruiting,
                                  trial_locations_total_countries_cat, trial_locations_total_regions, trial_multicentric,
                                  trial_multinational, trial_multiregional, starts_with('trial_regions'), starts_with('trial_income'),
                                  trial_cancer_type, trial_sponsor_type, trial_intervention_type),
      nonnormal = c('trial_locations_number', 'trial_locations_status_recruiting')))

# Single-center trials: where are they?
single_center_trials <- df_trials %>% dplyr::filter(trial_multicentric == 'single center') %>% pull(study_nct_id)
wb <- addStyledSheet(wb, 'trials_single_center', tableOneText(
      df %>% dplyr::filter(study_nct_id %in% single_center_trials) %>% dplyr::select(wdi_region, wdi_income_level, study_locations.country)))

# Multinational trials: regions and income groups involved
wb <- addStyledSheet(wb, 'trials_multinational', tableOneText(
      df_trials %>% dplyr::filter(trial_multinational == 'multinational') %>% dplyr::select(starts_with('trial_regions'), starts_with('trial_income'))))

# ============================================================================ #
# 2. Facility Level

df_centers <- summariseCenters(df)

# Characteristics, comparison across regions and top-100 facilities
wb <- writeCenterTables(df_centers, wb)

saveWorkbook(wb, "results/table_article_main.xlsx", overwrite = TRUE)

# ============================================================================ #
# 3. Country Level

# Countries with >=1 facility
df_country_centers <- summariseCountries(df)

# Trials vs facilities per country
tagPanels(plotTrialsFacilities(df_country_centers))
ggsave("plots/main_relationship_centers_trials.pdf", width = 21, height = 11, unit = 'cm', device = cairo_pdf)

# Linear models: trials ~ facilities (m1), log-log (m2), log-log without the US (m3)
models <- list(
      m1 = lm(country_n_trials ~ country_n_centers, data = df_country_centers),
      m2 = lm(log(country_n_trials) ~ log(country_n_centers), data = df_country_centers),
      # Note: '!= us' also drops Guam (no ISO code), so m3 has 83 countries
      m3 = lm(log(country_n_trials) ~ log(country_n_centers), data = df_country_centers %>% dplyr::filter(countries_code_iso2c != 'us')))

lapply(models, modelChecks)
compare_performance(models$m1, models$m2, models$m3, rank = TRUE)

saveDiagnostics(models$m1, 'plots/main_diagnostics_lm1_non_log.pdf')
saveDiagnostics(models$m2, 'plots/main_diagnostics_lm1_log.pdf')
saveDiagnostics(models$m3, 'plots/main_diagnostics_lm1_log_without_us.pdf')

# Adding UN member states without facilities (0 facilities). Countries without
# World Bank population (e.g. Taiwan) are excluded from here on.
df_country_centers <- addCountriesWithoutCenters(df_country_centers)

# Facilities per million inhabitants across World Bank regions
df_country_centers %>%
      dplyr::select(country_n_center_per_million, wdi_region) %>%
      gtsummary::tbl_summary(by = 'wdi_region') %>%
      gtsummary::add_p()

# Correlation between facilities and World Bank indicators
tagPanels(plotIndicatorCorrelations(df_country_centers))
ggsave("plots/main_correlation_centers_wdi_indicators.pdf", width = 21, height = 29, unit = "cm")

# Top 20 countries by number of facilities (A) and facilities per million (B)
tagPanels(plotTop20(df_country_centers, 'country_n_centers') | plotTop20(df_country_centers, 'country_n_center_per_million'))
ggsave("plots/main_top20_countries_facilities.pdf", width = 29, height = 17, unit = 'cm')

# Maps: facilities, and country rank by number (A) and density (B) of facilities
ggsave(plot = plotCentersMap(df_centers), "plots/main_map_centers_distribution.pdf", width = 24, height = 12, unit = 'cm')

tagPanels(plotCountryRankMaps(df_country_centers))
ggsave("plots/main_maps_country_number_density_distribution.pdf", width = 21, height = 21, unit = 'cm')

# ============================================================================ #
# 4. Unidentified Sites

# Odds ratios (95% CI) of a site being unidentified, by region and sponsor type
unidentifiableModels(df)

ggsave(plot = plotUnidentifiedMap(df), "plots/main_map_unidentifiable_locations.pdf", width = 24, height = 12, unit = 'cm')
