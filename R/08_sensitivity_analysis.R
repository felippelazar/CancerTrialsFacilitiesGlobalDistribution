# ============================================================================ #
# 8. Sensitivity Analysis: Sites Re-matched by ZIP Code                        #
# Author: Felippe Lazar Neto, Universidade de São Paulo, 2025                  #
# ============================================================================ #
#
# Purpose
#   Repeats the facility- and country-level analyses of script 07 on the
#   dataset in which unidentified sites were re-matched to a facility by ZIP
#   code and country (script 06): 70,827 identified and 6,798 unidentified
#   trial x site pairs, instead of 64,018 and 13,607. Trial-level results do not
#   change and are not repeated. The same functions as in script 07 are used.
#   Comparison figures show the main analysis on top and the sensitivity
#   analysis below.
#
# Input  : data/post-processed/df_zip_rematched_150126.parquet          (script 06)
#          data/post-processed/df_merged_150126.parquet                 (script 06, for comparison figures)
#          data/post-processed/all_countries_trials_demographics.parquet (script 05)
# Output : results/table_article_sens.xlsx   facility tables
#          plots/sensitivity_*.pdf           figures
#          plots/comparison_*.pdf            main (top) vs sensitivity (bottom)
# ============================================================================ #

source('R_public/00_analysis_functions.R')

df_sens <- arrow::read_parquet('data/post-processed/df_zip_rematched_150126.parquet') %>% fixRegions()
df_main <- arrow::read_parquet('data/post-processed/df_merged_150126.parquet') %>% fixRegions()

# ============================================================================ #
# 1. Facility Level

df_centers_sens <- summariseCenters(df_sens)

wb <- writeCenterTables(df_centers_sens, createWorkbook())
saveWorkbook(wb, "results/table_article_sens.xlsx", overwrite = TRUE)

# ============================================================================ #
# 2. Country Level

df_country_centers_sens <- summariseCountries(df_sens)
df_country_centers_main <- summariseCountries(df_main)

# Trials vs facilities per country
tagPanels(plotTrialsFacilities(df_country_centers_sens))
ggsave("plots/sensitivity_relationship_centers_trials.pdf", width = 21, height = 11, unit = 'cm', device = cairo_pdf)

tagPanels(plotTrialsFacilities(df_country_centers_main) / plotTrialsFacilities(df_country_centers_sens))
ggsave("plots/comparison_relationship_centers_trials.pdf", width = 21, height = 21, unit = 'cm', device = cairo_pdf)

# Linear models: trials ~ facilities (m1), log-log (m2), log-log without the US (m3)
models_sens <- list(
      m1 = lm(country_n_trials ~ country_n_centers, data = df_country_centers_sens),
      m2 = lm(log(country_n_trials) ~ log(country_n_centers), data = df_country_centers_sens),
      m3 = lm(log(country_n_trials) ~ log(country_n_centers), data = df_country_centers_sens %>% dplyr::filter(countries_code_iso2c != 'us')))

lapply(models_sens, modelChecks)
compare_performance(models_sens$m1, models_sens$m2, models_sens$m3, rank = TRUE)

saveDiagnostics(models_sens$m1, 'plots/sensitivity_diagnostics_lm1_non_log.pdf')
saveDiagnostics(models_sens$m2, 'plots/sensitivity_diagnostics_lm1_log.pdf')
saveDiagnostics(models_sens$m3, 'plots/sensitivity_diagnostics_lm1_log_without_us.pdf')

# Adding UN member states without facilities
df_country_centers_sens <- addCountriesWithoutCenters(df_country_centers_sens)

df_country_centers_sens %>%
      dplyr::select(country_n_center_per_million, wdi_region) %>%
      gtsummary::tbl_summary(by = 'wdi_region') %>%
      gtsummary::add_p()

# Correlation with World Bank indicators
tagPanels(plotIndicatorCorrelations(df_country_centers_sens))
ggsave("plots/sensitivity_correlation_centers_wdi_indicators.pdf", width = 21, height = 29, unit = "cm")

# Top 20 countries
tagPanels(plotTop20(df_country_centers_sens, 'country_n_centers') | plotTop20(df_country_centers_sens, 'country_n_center_per_million'))
ggsave("plots/sensitivity_top20_countries_facilities.pdf", width = 29, height = 17, unit = 'cm')

# Maps
ggsave(plot = plotCentersMap(df_centers_sens), "plots/sensitivity_map_centers_distribution.pdf", width = 24, height = 12, unit = 'cm')

tagPanels(plotCentersMap(summariseCenters(df_main)) / plotCentersMap(df_centers_sens))
ggsave("plots/comparison_map_centers_distribution.pdf", width = 24, height = 24, unit = 'cm')

tagPanels(plotCountryRankMaps(df_country_centers_sens))
ggsave("plots/sensitivity_maps_country_number_density_distribution.pdf", width = 21, height = 21, unit = 'cm')

# ============================================================================ #
# 3. Sites Still Unidentified After Re-matching

unidentifiableModels(df_sens)

ggsave(plot = plotUnidentifiedMap(df_sens), "plots/sensitivity_map_unidentifiable_locations.pdf", width = 24, height = 12, unit = 'cm')

tagPanels(plotUnidentifiedMap(df_main) / plotUnidentifiedMap(df_sens))
ggsave("plots/comparison_map_unidentifiable_locations.pdf", width = 24, height = 24, unit = 'cm')
