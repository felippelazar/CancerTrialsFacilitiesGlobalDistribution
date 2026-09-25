> **Use of AI tools.** After the analysis was completed, we used Claude Code (Anthropic) to review the code only to improve its readability and reproducibility (comments, organisation of the scripts and removal of duplicated code). All analyses were designed and conducted by the researchers. The code as originally written by the researchers is available in the first commit of this repository.

# Global Distribution and Characteristics of Research Facilities Participating in Phase 3 Oncology Trials

R code for the cross-sectional analysis of the research facilities recruiting for phase 3 cancer trials registered on ClinicalTrials.gov.

Author: Felippe Lazar Neto, Universidade de São Paulo

## Overview

The pipeline downloads recruiting cancer trials from ClinicalTrials.gov, selects phase 3 interventional treatment or supportive care trials, identifies each trial site with the Google Places API, groups sites that belong to the same facility, adds World Bank country indicators and runs the analyses reported in the article.

| Step | Result |
|---|---|
| Recruiting cancer trials (23 July 2024) | 19,523 |
| Phase 3 interventional treatment or supportive care trials | 1,381 |
| Cancer trials after excluding 94 non-cancer trials | 1,287 |
| Trial x site pairs (identified / unidentified) | 77,625 (64,018 / 13,607) |
| Unique research facilities | 6,634 |

Each script starts with a header that describes its purpose, input and output files, and the numbers it produces, so that every step can be matched to the flowchart of the article.

## Scripts

Run the scripts in order from the project root (the folder that contains `R_public/`, `data/`, `plots/` and `results/`).

| Script | Description |
|---|---|
| `00_aux_functions.R` | Helper functions for the Google Places API (sourced by other scripts) |
| `00_analysis_functions.R` | Functions for tables, models and figures, shared by scripts 07 and 08 |
| `01_query_clintrialsgov.R` | Downloads recruiting cancer trials from the ClinicalTrials.gov API v2 |
| `02_filtering_data.R` | Builds site addresses and IDs and applies the trial selection criteria |
| `03_google_api.R` | Queries the Google Places API for each unique site |
| `04_tidying_locations.R` | Applies the manual review of Google matches and groups Place IDs of the same facility (sites within 1,000 m, classified by Gemini 3.0 Pro) |
| `05_country_indicators.R` | Downloads World Bank indicators (income level, region, population, GDP, health and research expenditure) |
| `06_merging_datasets.R` | Builds the main dataset and the sensitivity dataset (unidentified sites re-matched by ZIP code and country) |
| `07_data_analysis.R` | Main analysis: trial, facility and country level tables, linear and mixed-effects models, and figures |
| `08_sensitivity_analysis.R` | Repeats the facility and country level analyses on the sensitivity dataset and creates the comparison figures |

## Reproducibility notes

- **External sources change over time.** Scripts 01, 03 and 05 query ClinicalTrials.gov, the Google Places API and the World Bank API. Running them again returns different data. To reproduce the article, start from the saved files they produce and run scripts 02, 04, 06, 07 and 08.
- **Google Places API key.** Script 03 needs your own key (a paid service), set as an environment variable, for example in `~/.Renviron`:
  ```
  GOOGLE_PLACES_API_KEY=your_key
  ```
  No credentials are stored in this repository.
- **Locale.** The site IDs in script 02 depend on the sort order of text. The script sets `LC_COLLATE` to `en_US.UTF-8`, the locale used to create the saved files.
- **Data.** The trial data come from ClinicalTrials.gov, a public database, and can be downloaded with script 01. The raw data and intermediate files used in the article are not included in this repository, but we are happy to share them on request: please send an email to the corresponding author.

## Software

R 4.3.3. Main packages: tidyverse, arrow, httr, jsonlite, glue, stringi, countrycode, igraph, tableone, gtsummary, openxlsx, lme4, broom, broom.mixed, car, lmtest, performance, sf, rnaturalearth, rnaturalearthdata, patchwork, ggrepel, scales, readxl, rio, tidylog, janitor.
