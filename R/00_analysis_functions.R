# ============================================================================ #
# 0. Analysis Functions                                                        #
# Author: Felippe Lazar Neto, Universidade de São Paulo, 2025                  #
# ============================================================================ #
#
# Functions shared by the main (07) and sensitivity (08) analyses, so that both
# are computed in exactly the same way. Sourced by scripts 07 and 08.
#
#   Data            : fixRegions(), summariseTrials(), summariseCenters(),
#                     summariseCountries(), addCountriesWithoutCenters()
#   Tables          : tableOneText(), addStyledSheet(), case_when_fct()
#   Models          : modelChecks(), saveDiagnostics(), unidentifiableModels()
#   Figures         : plotTrialsFacilities(), plotIndicatorCorrelations(),
#                     plotTop20(), plotCentersMap(), plotCountryRankMaps(),
#                     plotUnidentifiedMap()
# ============================================================================ #

library(tidyverse)
library(tableone)
library(openxlsx)
library(broom)
library(broom.mixed)
library(car)
library(lmtest)
library(performance)
library(lme4)
library(patchwork)
library(sf)
library(rnaturalearth)
library(rnaturalearthdata)

# Sites that could not be assigned to a facility (script 04)
unidentifiable <- c('Generic Site Name', 'Google Unfound Location', 'Manual Review Exclusion')

# World map (Natural Earth) used by all maps. Countries are joined by the
# 'adm0_a3' code, which matches ISO3 for all countries in the data ('su_a3'
# does not: Portugal is coded 'PR1').
s_world <- ne_countries(scale = 'medium')

# ============================================================================ #
# Tables

# case_when() returning a factor whose levels follow the order of the conditions
case_when_fct <- function(...) {
      dots <- rlang::enquos(...)
      rhs_values <- purrr::map(dots, function(q) {
            formula <- rlang::quo_get_expr(q)
            if (rlang::is_formula(formula)) rlang::eval_tidy(rlang::f_rhs(formula)) else NULL
      })
      factor(dplyr::case_when(...), levels = unique(unlist(rhs_values)))
}

# Descriptive table (tableone) returned as a character matrix. 0/1 variables are
# shown as categorical; 'nonnormal' variables as median [IQR]; with 'strata',
# groups are compared (chi-square/Kruskal-Wallis; Fisher for 'exact' variables).
tableOneText <- function(data, vars = names(data), strata = NULL, nonnormal = NULL, exact = NULL){
      data <- data %>% dplyr::mutate(across(where(is.numeric), ~ {if(all(. %in% c(0, 1, NA))) factor(.) else .}))
      table_args <- list(vars = vars, data = data, includeNA = FALSE)
      if(!is.null(strata)) table_args$strata <- strata
      do.call(CreateTableOne, table_args) %>%
            print(showAllLevels = FALSE, nonnormal = nonnormal, exact = exact, noSpaces = TRUE, quote = FALSE, printToggle = FALSE)
}

# Writes a table to a new, formatted sheet of an openxlsx workbook
addStyledSheet <- function(workbook, sheet_name, temp_table){

      temp_table <- data.frame(var_names = row.names(temp_table), temp_table)
      temp_table <- temp_table %>% mutate(abs_n = str_extract(.[[2]], '([0-9]*)', group = 1))

      cellStyle <- function(fill, halign, num_fmt = "0.000") createStyle(fontName = 'Arial', fontSize = 11, valign = 'center', halign = halign,
                                                                      fgFill = fill, border = "TopBottomLeftRight", numFmt = num_fmt, wrapText = T)
      header_style <- createStyle(fontName = 'Arial', fontSize = 12, textDecoration = "bold", valign = 'center', halign = 'center',
                                  fgFill = '#D3D3D3', border = "TopBottomLeftRight", wrapText = T)

      n_col <- ncol(temp_table); max_row <- nrow(temp_table) + 1
      rows_odd <- seq(2, max_row)[seq(2, max_row) %% 2 == 1]
      rows_even <- seq(2, max_row)[seq(2, max_row) %% 2 == 0]

      addWorksheet(workbook, sheet_name)
      writeData(workbook, sheet = sheet_name, x = temp_table, headerStyle = header_style)
      addStyle(workbook, sheet_name, style = cellStyle('#FFFFFF', 'left'), rows = rows_even, cols = 1, gridExpand = T)
      addStyle(workbook, sheet_name, style = cellStyle('#e4e9f2', 'left'), rows = rows_odd, cols = 1, gridExpand = T)
      addStyle(workbook, sheet_name, style = cellStyle('#FFFFFF', 'center'), rows = rows_even, cols = 2:n_col, gridExpand = T)
      addStyle(workbook, sheet_name, style = cellStyle('#e4e9f2', 'center'), rows = rows_odd, cols = 2:n_col, gridExpand = T)
      col_widths <- sapply(temp_table, function(x) min(max(nchar(x), na.rm = T) * 1.1, 40))
      setColWidths(workbook, sheet_name, cols = seq_along(temp_table), widths = col_widths)
      setRowHeights(workbook, sheet_name, rows = 2:max_row, heights = 18)

      return(workbook)
}

# ============================================================================ #
# Data

# 1 if >=1 / all element(s) TRUE, else 0. As in sum(x) > 0, the result is NA
# when x contains NA (e.g. a site in a country without World Bank income level)
hasAny <- function(x) ifelse(sum(x) > 0, 1, 0)
hasAll <- function(x) ifelse(all(x), 1, 0)

# World Bank has no region for Taiwan, Martinique and Guam: assigned manually
fixRegions <- function(df){
      df %>%
            dplyr::mutate(wdi_region = case_when(
                  study_locations.country %in% c('Taiwan', 'Guam') ~ 'East Asia & Pacific',
                  study_locations.country == 'Martinique' ~ 'Latin America & Caribbean ', # trailing space as in World Bank labels
                  TRUE ~ wdi_region))
}

# Trial level: one row per trial. Multiregional = sites in >1 World Bank region.
summariseTrials <- function(df){
      df %>%
            dplyr::group_by(study_nct_id) %>%
            dplyr::summarise(
                  trial_locations_number = length(unique(id_facility_full_address)),
                  trial_locations_number_cat = case_when_fct(
                        trial_locations_number %in% 1 ~ '1 Sites',
                        trial_locations_number %in% 2:5 ~ '2-5 Sites',
                        trial_locations_number %in% 6:10 ~ '6-10 Sites',
                        trial_locations_number %in% 11:20 ~ '11-20 Sites',
                        trial_locations_number %in% 21:50 ~ '21-50 Sites',
                        trial_locations_number > 50 ~ '51+ Sites'),
                  trial_locations_status_recruiting = sum(study_locations.status == 'RECRUITING'),
                  trial_locations_total_countries = length(unique(study_locations.country)),
                  trial_locations_total_countries_cat = factor(
                        ifelse(trial_locations_total_countries >= 10, '10+ Countries',
                               paste(trial_locations_total_countries, ifelse(trial_locations_total_countries == 1, 'Country', 'Countries'))),
                        levels = c('1 Country', paste(2:9, 'Countries'), '10+ Countries')),
                  trial_locations_total_regions = as.factor(length(unique(wdi_region))),
                  trial_multicentric = factor(ifelse(trial_locations_number == 1, 'single center', 'multicentric'), levels = c('single center', 'multicentric')),
                  trial_multinational = factor(ifelse(trial_locations_total_countries == 1, 'single country', 'multinational'), levels = c('single country', 'multinational')),
                  trial_multiregional = factor(ifelse(trial_locations_total_regions == 1, 'single region', 'multiregional'), levels = c('single region', 'multiregional')),

                  # 1 if the trial has >=1 site in the region / income group
                  trial_regions_latin_america = hasAny(wdi_region == 'Latin America & Caribbean '),
                  trial_regions_north_america = hasAny(wdi_region == 'North America'),
                  trial_regions_europe_central_asia = hasAny(wdi_region == 'Europe & Central Asia'),
                  trial_regions_east_asia_pacific = hasAny(wdi_region == 'East Asia & Pacific'),
                  trial_regions_subsaharan_africa = hasAny(wdi_region == 'Sub-Saharan Africa '),
                  trial_regions_south_asia = hasAny(wdi_region == 'South Asia'),
                  trial_regions_middle_east_north_africa = hasAny(wdi_region == 'Middle East, North Africa, Afghanistan & Pakistan'),
                  trial_income_high_income = hasAny(wdi_income_level == 'High income'),
                  trial_income_upper_middle_income = hasAny(wdi_income_level == 'Upper middle income'),
                  trial_income_lower_middle_income = hasAny(wdi_income_level == 'Lower middle income'),
                  trial_income_low_income = hasAny(wdi_income_level == 'Low income'),

                  trial_cancer_type = first(new_cancer_category_code),
                  trial_sponsor_type = first(new_study_lead_sponsor_type),
                  trial_intervention_type = first(new_intervention_type)
            )
}

# Facility level: one row per identified facility (Google Place ID).
# 'any' = >=1 trial with the feature; 'exclusive' = all trials with the feature.
summariseCenters <- function(df){
      df %>%
            dplyr::group_by(study_nct_id) %>%
            dplyr::mutate(
                  trial_single_center = length(unique(id_facility_full_address)) == 1,
                  trial_multinational = length(unique(study_locations.country)) > 1,
                  trial_multiregional = length(unique(wdi_region)) > 1) %>%
            dplyr::ungroup() %>%
            dplyr::filter(!candidates.place_id %in% unidentifiable) %>%
            dplyr::group_by(candidates.place_id) %>%
            dplyr::summarise(
                  center_candidates_name = first(candidates.name),
                  center_candidates_address = first(candidates.formatted_address),
                  across(starts_with('wdi_'), first),
                  center_locations_country = first(study_locations.country),
                  countries_code_iso2c = first(countries_code_iso2c),
                  countries_code_iso3c = first(countries_code_iso3c),
                  center_locations_region = first(study_locations.country_region),

                  center_n_total_trials = length(unique(study_nct_id)),
                  center_n_total_cat = case_when_fct(
                        center_n_total_trials %in% 1:5 ~ '1-5 Trials',
                        center_n_total_trials %in% 6:10 ~ '6-10 Trials',
                        center_n_total_trials %in% 11:20 ~ '11-20 Trials',
                        center_n_total_trials %in% 21:50 ~ '21-50 Trials',
                        center_n_total_trials > 50 ~ '51+ Trials'),
                  center_n_recruiting = sum(study_locations.status == 'RECRUITING'),

                  center_any_single_center = hasAny(trial_single_center),
                  center_any_radiotherapy = hasAny(new_intervention_type == 'radiotherapy'),
                  center_any_surgery = hasAny(new_intervention_type == 'surgery'),
                  center_exclusive_multicenter = hasAll(!trial_single_center),
                  center_exclusive_multinational = hasAll(trial_multinational),
                  center_exclusive_multiregional = hasAll(trial_multiregional),
                  center_exclusive_industry = hasAll(new_study_lead_sponsor_type == 'INDUSTRY'),
                  center_exclusive_systemic = hasAll(new_intervention_type == 'systemic treatment'),

                  # Median, as sites re-matched by ZIP code (script 06) have no coordinates
                  center_lat = median(candidates.geometry.location.lat, na.rm = TRUE),
                  center_lng = median(candidates.geometry.location.lng, na.rm = TRUE)
            )
}

# Country level: countries with >=1 identified facility
summariseCountries <- function(df){
      df %>%
            dplyr::filter(!candidates.place_id %in% unidentifiable) %>%
            dplyr::group_by(study_locations.country) %>%
            dplyr::summarise(
                  across(starts_with('wdi_'), first),
                  country_n_centers = length(unique(candidates.place_id)),
                  country_n_trials = length(unique(study_nct_id)),
                  country_study_locations_region = first(study_locations.country_region),
                  country_n_center_per_million = country_n_centers / (wdi_total_population_value / 1000000),
                  countries_code_iso2c = tolower(first(countries_code_iso2c)),
                  countries_code_iso3c = tolower(first(countries_code_iso3c)))
}

# Adds the UN member states without any facility (0 facilities, 0 trials)
addCountriesWithoutCenters <- function(df_countries){
      df_other_countries <- arrow::read_parquet('data/post-processed/all_countries_trials_demographics.parquet') %>%
            dplyr::rename(countries_code_iso3c = countryiso3code) %>%
            dplyr::anti_join(df_countries, by = "countries_code_iso3c")

      df_countries %>%
            dplyr::bind_rows(df_other_countries) %>%
            dplyr::mutate(
                  country_n_centers = replace_na(country_n_centers, 0),
                  country_n_trials = replace_na(country_n_trials, 0),
                  country_n_center_per_million = country_n_centers / (wdi_total_population_value / 1000000)) %>%
            dplyr::filter(!is.na(wdi_total_population_value))
}

# ============================================================================ #
# Tables of the Article (one Excel sheet each)

centerVars <- c('center_n_total_trials', 'center_n_total_cat', 'center_n_recruiting', 'center_any_single_center',
                'center_any_radiotherapy', 'center_any_surgery', 'center_exclusive_multicenter', 'center_exclusive_multinational',
                'center_exclusive_multiregional', 'center_exclusive_industry', 'center_exclusive_systemic')

writeCenterTables <- function(df_centers, wb){
      wb <- addStyledSheet(wb, 'centers_characteristics',
                           tableOneText(df_centers, c('center_locations_country', 'wdi_region', 'wdi_income_level', centerVars),
                                        nonnormal = c('center_n_total_trials', 'center_n_recruiting')))
      wb <- addStyledSheet(wb, 'centers_comparison_regions',
                           tableOneText(df_centers, centerVars, strata = 'wdi_region',
                                        nonnormal = c('center_n_total_trials', 'center_n_recruiting'),
                                        exact = c('center_any_single_center', 'center_exclusive_multicenter', 'center_exclusive_multinational',
                                                  'center_exclusive_multiregional', 'center_exclusive_industry', 'center_exclusive_systemic')))
      wb <- addStyledSheet(wb, 'top100_centers',
                           tableOneText(df_centers %>% dplyr::arrange(desc(center_n_total_trials)) %>% slice_head(n = 100),
                                        c('center_n_total_trials', 'center_locations_country', 'center_n_total_cat', 'wdi_region', 'wdi_income_level'),
                                        nonnormal = 'center_n_total_trials'))
      return(wb)
}

# ============================================================================ #
# Models

# Linear model results and assumption checks
modelChecks <- function(model){
      list(
            coefficients = tidy(model, conf.int = TRUE),
            fit = glance(model),
            performance = model_performance(model),
            shapiro_residuals = shapiro.test(residuals(model)),
            breusch_pagan = bptest(model),
            ncv = ncvTest(model),
            outliers = outlierTest(model))
}

saveDiagnostics <- function(model, file){
      pdf(file, width = 8, height = 8)
      par(mfrow = c(2, 2))
      plot(model)
      dev.off()
}

# Factors associated with an unidentified site: mixed logistic regression with
# a random intercept per trial (sites of the same trial are correlated)
unidentifiableModels <- function(df){
      df_loc <- df %>% dplyr::mutate(unidentifiable = as.numeric(candidates.place_id %in% unidentifiable))
      models <- list(
            region = unidentifiable ~ relevel(factor(wdi_region), "North America") + (1 | study_nct_id),
            sponsor = unidentifiable ~ relevel(factor(new_study_lead_sponsor_type), "GOVERNMENT_AGENCIES") + (1 | study_nct_id),
            multivariable = unidentifiable ~ relevel(factor(wdi_region), "North America") + relevel(factor(new_study_lead_sponsor_type), "GOVERNMENT_AGENCIES") + (1 | study_nct_id))
      # Multivariable model fitted with the bobyqa optimizer (the default did not converge)
      controls <- list(region = glmerControl(), sponsor = glmerControl(),
                       multivariable = glmerControl(optimizer = "bobyqa", optCtrl = list(maxfun = 2e5)))
      lapply(names(models), function(k) {
            glmer(models[[k]], data = df_loc, family = 'binomial', control = controls[[k]]) %>%
                  tidy(effects = "fixed", conf.int = TRUE, exponentiate = TRUE) %>%
                  dplyr::mutate(ci95 = sprintf("%.2f (95%%CI %.2f–%.2f)", estimate, conf.low, conf.high)) %>%
                  dplyr::select(term, ci95)
      }) %>% setNames(names(models))
}

# ============================================================================ #
# Figures

# Trials vs facilities per country: (A) linear and (B) log scale
plotTrialsFacilities <- function(df_countries){
      base <- df_countries %>%
            ggplot(aes(x = country_n_centers, y = country_n_trials, label = toupper(countries_code_iso3c))) +
            geom_point() +
            theme_bw(base_family = 'Helvetica') +
            theme(legend.position = 'top', axis.text.y = element_text(face = "bold"), text = element_text(size = 10))
      p_non_log <- base +
            ggrepel::geom_text_repel(size = 4, color = 'darkgray', point.padding = 0.5) +
            labs(x = 'Research Facilities (N)', y = 'Phase III Cancer Trials (N)') +
            theme(axis.text.x = element_text(face = "bold"))
      p_log <- base +
            ggrepel::geom_text_repel(size = 4, color = 'darkgray') +
            labs(x = 'Research Facilities (log-scale)', y = 'Phase III Cancer Trials (log-scale)') +
            scale_x_log10() + scale_y_log10()
      p_non_log | p_log
}

# Correlation of facilities with each World Bank indicator, all UN member
# states. Pearson (Fisher z CI) and Spearman (Bonett & Wright, 2000 CI),
# on the (A) linear and (B) log10(x + 1) scales.
indicatorsLong <- function(df_countries){
      df_countries %>%
            dplyr::select(-wdi_income_level, -wdi_region, -wdi_human_capital_index_value, -wdi_oop_health_expenditure_value,
                          -wdi_nurses_per_1000_value, -wdi_hospital_beds_per_1000_value, -matches('date')) %>%
            tidyr::pivot_longer(cols = matches('wdi'), names_to = 'wdi_indicator', values_to = 'wdi_value') %>%
            dplyr::mutate(wdi_indicator = gsub("Gdp", "GDP", stringr::str_to_title(gsub("_|wdi", " ", wdi_indicator))))
}

correlationLabels <- function(df_plot, log_scale = FALSE){
      if(log_scale) df_plot <- df_plot %>% dplyr::mutate(wdi_value = log10(wdi_value + 1), country_n_centers = log10(country_n_centers + 1))
      df_plot %>%
            dplyr::filter(is.finite(wdi_value), is.finite(country_n_centers)) %>%
            dplyr::group_by(wdi_indicator) %>%
            dplyr::summarise(n = n(),
                             r = cor(wdi_value, country_n_centers, method = "pearson"),
                             rho = cor(wdi_value, country_n_centers, method = "spearman"), .groups = "drop") %>%
            dplyr::mutate(
                  r_lo = tanh(atanh(r) - qnorm(0.975) / sqrt(n - 3)),
                  r_hi = tanh(atanh(r) + qnorm(0.975) / sqrt(n - 3)),
                  rho_se = sqrt((1 + rho^2 / 2) / (n - 3)),
                  rho_lo = tanh(atanh(rho) - qnorm(0.975) * rho_se),
                  rho_hi = tanh(atanh(rho) + qnorm(0.975) * rho_se),
                  label = sprintf("Pearson: r = %.2f (95%% CI %.2f to %.2f)\nSpearman: ρ = %.2f (95%% CI %.2f to %.2f)", r, r_lo, r_hi, rho, rho_lo, rho_hi))
}

plotIndicatorCorrelations <- function(df_countries){
      df_plot <- indicatorsLong(df_countries)
      panel <- function(p, labels, x_lab, y_lab, label_x) {
            p + geom_point(alpha = 0.6, size = 2, color = "#2C3E50") +
                  geom_smooth(method = "loess", formula = y ~ x, se = TRUE, color = "#E74C3C") +
                  geom_text(data = labels, aes(x = label_x, y = Inf, label = label), inherit.aes = FALSE,
                            hjust = -0.03, vjust = 1.2, size = 2.5, lineheight = 0.95, family = "Helvetica") +
                  facet_wrap(~wdi_indicator, scales = "free", nrow = 9) +
                  labs(y = y_lab, x = x_lab) +
                  theme_minimal(base_family = "Helvetica") +
                  theme(axis.title = element_text(face = "bold", size = 11), strip.text = element_text(face = "bold", size = 9),
                        strip.background = element_rect(fill = "grey90", color = "grey50"), panel.grid.minor = element_blank(),
                        panel.border = element_rect(color = "grey80", fill = NA))
      }
      p_non_log <- panel(ggplot(df_plot, aes(y = country_n_centers, x = wdi_value)), correlationLabels(df_plot),
                         "Indicator Value", "Number of Centers", -Inf)
      p_log <- panel(ggplot(df_plot, aes(y = country_n_centers + 1, x = wdi_value + 1)) + scale_x_log10() + scale_y_log10(),
                     correlationLabels(df_plot, log_scale = TRUE),
                     "Indicator Value + 1 (log10)", "Number of Centers + 1 (log10)", 0) # x = 0 is the left edge on log10
      p_non_log | p_log
}

# Top 20 countries by number of facilities or by facilities per million
plotTop20 <- function(df_countries, order_by = c('country_n_centers', 'country_n_center_per_million')){
      order_by <- match.arg(order_by)
      region_abbreviations <- c("East Asia & Pacific" = "EAP", "Europe & Central Asia" = "ECA", "Latin America & Caribbean " = "LAC",
                                "North America" = "NAM", "South Asia" = "SAR")
      region_colors <- c("East Asia & Pacific" = "#FDD2D2", "Europe & Central Asia" = "#ffd7b5", "Latin America & Caribbean " = "#C3EAD5",
                         "North America" = "#BFE0F7", "South Asia" = "#E8C2F2")
      df_countries %>%
            dplyr::arrange(desc(.data[[order_by]])) %>%
            head(20) %>%
            dplyr::mutate(
                  country_code_upper = forcats::fct_reorder(toupper(countries_code_iso3c), .data[[order_by]]),
                  `Country Code` = as.character(country_code_upper),
                  `Number of Facilities` = scales::number(country_n_centers, accuracy = 1),
                  `Total Population` = scales::comma(wdi_total_population_value, accuracy = 1),
                  `Research Density` = scales::number(country_n_center_per_million, accuracy = 0.01),
                  `WDI Region` = as.character(wdi_region)) %>%
            pivot_longer(cols = c(`Country Code`, `Number of Facilities`, `Total Population`, `Research Density`), names_to = 'metric', values_to = 'value') %>%
            ggplot(aes(y = country_code_upper, x = metric, fill = `WDI Region`)) +
            geom_tile(alpha = 0.9, color = 'black') +
            geom_text(aes(label = value), size = 3.5) +
            theme_minimal() +
            theme(legend.position = 'bottom', axis.text.y = element_blank(), axis.ticks.y = element_blank(),
                  panel.grid.major.y = element_blank(), plot.margin = unit(c(1, 1, 1, 1), "cm")) +
            labs(y = "", fill = "", x = "") +
            scale_x_discrete(position = "top") +
            scale_fill_manual(values = region_colors, labels = region_abbreviations)
}

# Common look of the world maps
mapTheme <- function(){
      theme_bw(base_size = 10, base_family = "Helvetica") +
            theme(panel.background = element_rect(fill = '#DDEEFF', color = "black", linewidth = 0.5),
                  plot.background = element_rect(fill = 'gray99'),
                  panel.grid = element_blank(), panel.border = element_blank(),
                  axis.title = element_blank(), axis.text = element_blank(), axis.ticks = element_blank(),
                  legend.position = c(0.5, 0.1), legend.direction = 'horizontal', legend.title = element_blank(),
                  legend.background = element_rect(fill = "white", color = "black", linewidth = 0.2),
                  legend.spacing.y = unit(0.2, 'cm'))
}

# Location of each facility, coloured and sized by number of trials
plotCentersMap <- function(df_centers){
      sf_plot <- df_centers %>%
            semi_join(s_world, by = c('countries_code_iso3c' = 'adm0_a3')) %>%
            dplyr::filter(!is.na(center_lng)) %>%
            dplyr::arrange(center_n_total_trials) %>%
            st_as_sf(coords = c("center_lng", "center_lat"), crs = 4326)
      ggplot() +
            geom_sf(data = s_world, fill = "#FEFDED", color = "gray30", linewidth = 0.1) +
            geom_sf(data = sf_plot, aes(fill = center_n_total_cat, size = center_n_total_cat),
                    shape = 21, color = 'black', stroke = 0.5, alpha = 0.4) +
            scale_fill_viridis_d(option = "D") +
            scale_size_discrete(range = c(2, 4)) +
            labs(fill = "Facility Size", size = "Facility Size") +
            mapTheme()
}

# Country rank by (A) number of facilities and (B) facilities per million
plotCountryRankMaps <- function(df_countries){
      df_plot <- s_world %>%
            left_join(df_countries %>% mutate(countries_code_iso3c = toupper(countries_code_iso3c)), by = c('adm0_a3' = 'countries_code_iso3c')) %>%
            arrange(desc(country_n_centers)) %>%
            mutate(rank_country_n_centers = ifelse(is.na(country_n_centers), NA, row_number())) %>%
            arrange(desc(country_n_center_per_million)) %>%
            mutate(rank_country_n_center_per_million = ifelse(is.na(country_n_center_per_million), NA, row_number()))
      rankMap <- function(var) {
            ggplot() +
                  geom_sf(data = df_plot, aes(fill = .data[[var]]), color = "gray30", linewidth = 0.1) +
                  scale_fill_distiller(palette = 'YlOrRd', direction = -1, na.value = "white") +
                  mapTheme()
      }
      rankMap('rank_country_n_centers') / rankMap('rank_country_n_center_per_million')
}

# Percentage of unidentified sites per country
plotUnidentifiedMap <- function(df){
      df_country <- df %>%
            dplyr::group_by(study_locations.country) %>%
            dplyr::summarise(
                  perc_unidentifiable = mean(candidates.place_id %in% unidentifiable) * 100,
                  countries_code_iso3c = first(countries_code_iso3c)) %>%
            dplyr::mutate(perc_unidentifiable_cat = cut(perc_unidentifiable, breaks = c(-Inf, 0, 25, 50, 75, Inf),
                                                        labels = c('0%', '1-25%', '26-50%', '51-75%', '76-100%')))
      df_plot <- s_world %>% left_join(df_country %>% mutate(countries_code_iso3c = toupper(countries_code_iso3c)), by = c('adm0_a3' = 'countries_code_iso3c'))
      ggplot() +
            geom_sf(data = df_plot, aes(fill = perc_unidentifiable_cat), color = "gray30", linewidth = 0.1) +
            scale_fill_brewer(palette = 'Purples', direction = 1, na.value = "gray90") +
            labs(fill = "% Unidentified") +
            mapTheme() + theme(legend.title = element_text())
}

# Tags panels A, B, ... and sets the tag font
tagPanels <- function(p){
      p + plot_annotation(tag_levels = "A") & theme(plot.tag = element_text(face = "bold", size = 16, family = "Helvetica"))
}
