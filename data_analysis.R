# Loading libraries
library(tidyverse)
library(janitor)
library(readxl)
library(openssl)
library(data.table)
library(clipr)
library(forcats)
library(arrow)
library(googlesheets4)

# Read facility lookup
facility_lookup <- read_csv("facilities.csv") %>% 
  clean_names()

# 2024 data link
link_2025 <- "https://github.com/deportationdata/ice/raw/refs/heads/main/data/detention-stints-latest.feather"

# Function to read Excel
read_detention_data <- function(link) {
  tmpfile <- tempfile(fileext = ".feather")
  download.file(link, destfile = tmpfile, mode = "wb")
  df <- read_feather(tmpfile) %>% clean_names()
  unlink(tmpfile)
  df
}

# Load data
detention_2025 <- read_detention_data(link_2025)

# Clean and prepare data
data_clean <- detention_2025 %>%
  left_join(facility_lookup, by = "detention_facility_code") %>%
  mutate(
    # Convert dates
    stay_in = as.Date(stay_book_in_date_time),
    stay_out = as.Date(stay_book_out_date),
    detention_in = as.Date(book_in_date_time),
    detention_out = as.Date(book_out_date_time),
    
    # Calculate lengths
    stay_length = as.numeric(difftime(stay_out, stay_in, units = "days")),
    detention_length = as.numeric(difftime(detention_out, detention_in, units = "days")),
    
    # Extract years
    stay_in_year = year(stay_in),
    stay_out_year = year(stay_out),
    detention_in_year = year(detention_in),
    detention_out_year = year(detention_out),
    
    # Create quarter-year labels
    stay_in_quarter_year = paste0(year(stay_in), " Q", quarter(stay_in)),
    stay_out_quarter_year = paste0(year(stay_out), " Q", quarter(stay_out)),
    detention_in_quarter_year = paste0(year(detention_in), " Q", quarter(detention_in)),
    detention_out_quarter_year = paste0(year(detention_out), " Q", quarter(detention_out)),
    
    # Calculate ages - using low/high because we only have birth year
    stay_in_age_low = if_else(!is.na(stay_in_year) & !is.na(birth_year), stay_in_year - birth_year, NA_integer_),
    stay_in_age_high = stay_in_age_low - 1,
    stay_out_age_low = if_else(!is.na(stay_out_year) & !is.na(birth_year), stay_out_year - birth_year, NA_integer_),
    stay_out_age_high = stay_out_age_low - 1,
    detention_in_age_low = if_else(!is.na(detention_in_year) & !is.na(birth_year), detention_in_year - birth_year, NA_integer_),
    detention_in_age_high = detention_in_age_low - 1,
    detention_out_age_low = if_else(!is.na(detention_out_year) & !is.na(birth_year), detention_out_year - birth_year, NA_integer_),
    detention_out_age_high = detention_out_age_low - 1,
    
    # Create groupings
    LA_vs_all = if_else(state == "LA", "LA", "Other"),
    age_category = case_when(
      detention_in_age_low < 20 ~ "< 20",
      detention_in_age_low < 40 ~ "20 - 40",
      detention_in_age_low < 60 ~ "40 - 60",
      detention_in_age_low >= 60 ~ "> 60",
      TRUE ~ NA_character_
    )
  ) %>%
  # Filter to 2025 only
  filter(detention_in >= as.Date("2025-01-01") & detention_in < as.Date("2026-01-01"))

# ----------------------------- Transfers ----------------------------------

# Build transfer tracking dataframe
transfer <- data_clean %>%
  filter(!is.na(unique_identifier)) %>%
  mutate(stayid = md5(paste0(stay_book_in_date_time, unique_identifier))) %>%
  
  # Remove duplicates
  group_by(stay_book_in_date_time, book_in_date_time, unique_identifier) %>%
  mutate(count = n()) %>%
  filter(count == 1) %>%
  select(-count) %>%
  ungroup() %>%
  
  # Number detentions within each stay
  group_by(stayid) %>%
  arrange(book_in_date_time) %>%
  mutate(
    detention_count = rleid(book_in_date_time),
    n_detentions = max(detention_count)
  ) %>%
  ungroup() %>%
  arrange(stayid, book_in_date_time) %>%
  
  # Figure out if they moved facilities
  mutate(
    moved = case_when(
      lead(detention_count) > detention_count & lead(stayid) == stayid ~ "Yes",
      is.na(book_out_date_time) ~ "Active",
      lead(detention_count) <= detention_count ~ "No",
      TRUE ~ NA_character_
    ),
    move_type = case_when(
      detention_release_reason == "Transferred" & moved == "Yes" ~ "Transferred",
      detention_release_reason == "Transferred" ~ "Transferred but not moved",
      moved == "Yes" ~ "Moved for another reason",
      moved == "Active" ~ "Active",
      TRUE ~ "Not moved"
    ),
    move_location = if_else(
      move_type %in% c("Transferred", "Moved for another reason"),
      lead(detention_facility_code),
      NA_character_
    )
  ) %>%
  select(
    stayid, stay_book_in_date_time, n_detentions, unique_identifier,
    book_in_date_time, book_out_date_time, detention_facility_code,
    detention_facility, detention_release_reason, detention_count,
    state, moved, move_type, move_location
  )

# ----------------------------- Summarization Function ----------------------------------

# Function to summarize data for a single facility
summarize_facility_data <- function(facility_name) {
  facility_data <- filter(data_clean, detention_facility == facility_name)
  
  # Unique people
  n_unique <- n_distinct(facility_data$unique_identifier)
  
  # Rank among all facilities
  rank <- data_clean %>%
    count(detention_facility, name = "n") %>%
    arrange(desc(n)) %>%
    mutate(rank = row_number()) %>%
    filter(detention_facility == facility_name) %>%
    pull(rank)
  
  # Percent with no criminal conviction
  total_records <- nrow(facility_data)
  pct_crim <- round(
    ifelse(total_records > 0,
           sum(facility_data$book_in_criminality != "1 Convicted Criminal", na.rm = TRUE) / total_records * 100,
           NA_real_), 2
  )
  
  # Average detention length
  mean_length <- round(mean(facility_data$detention_length, na.rm = TRUE), 2)
  
  # Count over 60 days
  over_60 <- nrow(filter(facility_data, detention_length > 60))
  
  # Top 5 departure countries
  departure_country_summary <- facility_data %>%
    filter(!is.na(departure_country)) %>%
    mutate(departure_country = str_to_title(departure_country)) %>%
    group_by(departure_country) %>%
    summarize(n = n_distinct(unique_identifier)) %>%
    arrange(-n) %>%
    slice_head(n = 5)
  
  # Top 5 transfer destinations
  transferred_cases <- transfer %>%
    filter(detention_facility == facility_name) %>%
    count(move_location, name = "n") %>%
    rename(detention_facility_code = move_location) %>%
    left_join(facility_lookup, by = "detention_facility_code") %>%
    filter(!is.na(detention_facility_name)) %>%
    arrange(desc(n)) %>%
    slice_head(n = 5) %>%
    select(detention_facility_name, n)
  
  # Age breakdown
  age_summary <- facility_data %>%
    mutate(age_category = factor(age_category, levels = c("< 20", "20 - 40", "40 - 60", "> 60"))) %>%
    group_by(age_category) %>%
    summarize(n = n_distinct(unique_identifier))
  
  # Ethnicity breakdown
  ethnicity_summary <- facility_data %>%
    mutate(ethnicity = case_when(
      ethnicity == "Hispanic Origin" ~ "Hispanic",
      ethnicity == "Not of Hispanic Origin" ~ "Not Hispanic",
      ethnicity == "Unknown" ~ "Unknown",
      TRUE ~ "None Reported"
    )) %>%
    group_by(ethnicity) %>%
    summarize(n = n_distinct(unique_identifier))
  
  # Gender breakdown
  gender_summary <- facility_data %>%
    group_by(gender) %>%
    summarize(n = n_distinct(unique_identifier))
  
  # Return everything as a list
  list(
    n_unique_people = n_unique,
    rank = rank,
    pct_convicted_criminal = pct_crim,
    avg_detention_length = mean_length,
    over_60 = over_60,
    top_5_departure_countries = departure_country_summary,
    top_5_transfer_destinations = transferred_cases,
    age_summary = age_summary,
    ethnicity_summary = ethnicity_summary,
    gender_summary = gender_summary
  )
}

# Run function on all Louisiana detention facilities
alexandria  <- summarize_facility_data("ALEXANDRIA STAGING FACILITY")
central     <- summarize_facility_data("CENTRAL LOUISIANA ICE PROC CTR")
south       <- summarize_facility_data("SOUTH LOUISIANA ICE PROC CTR")
pine        <- summarize_facility_data("PINE PRAIRIE ICE PROCESSING CENTER")
allen       <- summarize_facility_data("ALLEN PARISH PUBLIC SAFETY COMPLEX")
jackson     <- summarize_facility_data("JACKSON PARISH CORRECTIONAL CENTER")
richwood    <- summarize_facility_data("RICHWOOD COR CENTER")
winn        <- summarize_facility_data("WINN CORRECTIONAL CENTER")
river       <- summarize_facility_data("River Correctional Center")

# ----------------------------- State Breakdown ----------------------------------

# Filter to Louisiana
la_data <- filter(data_clean, state == "LA")
la_transfer <- filter(transfer, state == "LA")

# Unique people in LA
la_n_unique <- n_distinct(la_data$unique_identifier)

# LA rank among all states
la_rank <- data_clean %>%
  count(state, name = "n") %>%
  arrange(desc(n)) %>%
  mutate(rank = row_number()) %>%
  filter(state == "LA") %>%
  pull(rank)

# Percent with no criminal conviction
total_records <- nrow(la_data)
la_pct_crim <- round(sum(la_data$book_in_criminality != "1 Convicted Criminal", na.rm = TRUE) / total_records * 100, 2)

# Average detention length
la_mean_length <- round(mean(la_data$detention_length, na.rm = TRUE), 2)

# Count over 60 days
la_over_60 <- nrow(filter(la_data, detention_length > 60))

# Top 5 departure countries
la_countries <- la_data %>%
  filter(!is.na(departure_country)) %>%
  mutate(departure_country = str_to_title(departure_country)) %>%
  group_by(departure_country) %>%
  summarize(n = n_distinct(unique_identifier)) %>%
  arrange(-n) %>%
  slice_head(n = 5)

# Top 5 transfer destination states
la_transfers <- la_transfer %>%
  count(move_location, name = "facility_count") %>%
  rename(detention_facility_code = move_location) %>%
  left_join(facility_lookup, by = "detention_facility_code") %>%
  filter(!is.na(detention_facility_name)) %>%
  count(state, wt = facility_count, name = "total_transfers") %>%
  filter(state != "LA") %>%
  arrange(desc(total_transfers)) %>%
  slice_head(n = 5)

# Age breakdown
la_age <- la_data %>% 
  mutate(age_category = factor(age_category, levels = c("< 20", "20 - 40", "40 - 60", "> 60"))) %>%
  group_by(age_category) %>%
  summarize(n = n_distinct(unique_identifier))

# Ethnicity breakdown
la_ethnicity <- la_data %>%
  mutate(ethnicity = case_when(
    ethnicity == "Hispanic Origin" ~ "Hispanic",
    ethnicity == "Not of Hispanic Origin" ~ "Not Hispanic",
    ethnicity == "Unknown" ~ "Unknown",
    TRUE ~ "None Reported"
  )) %>%
  group_by(ethnicity) %>%
  summarize(n = n_distinct(unique_identifier))

# Gender breakdown
la_gender <- la_data %>%
  group_by(gender) %>%
  summarize(n = n_distinct(unique_identifier))

# ----------------------------- Export to Google Sheets ----------------------------------

# Helper function to format single values for sheets
create_value_df <- function(value, context) {
  data.frame(spacer = c("", ""), value = c(value, context))
}

# Consolidate all data into one list
all_data <- list(
  # Alexandria
  alexandria_n_unique = create_value_df(alexandria$n_unique_people, "20000"),
  alexandria_rank = create_value_df(alexandria$rank, ""),
  alexandria_pct_convicted_criminal = create_value_df(alexandria$pct_convicted_criminal, "100%"),
  alexandria_avg_detention_length = create_value_df(alexandria$avg_detention_length, "20 Days"),
  alexandria_over_60 = create_value_df(alexandria$over_60, "20 Days"),
  alexandria_countries = alexandria$top_5_departure_countries,
  alexandria_transfers = alexandria$top_5_transfer_destinations,
  alexandria_age = alexandria$age_summary,
  alexandria_ethnicity = alexandria$ethnicity_summary,
  alexandria_gender = alexandria$gender_summary,
  
  # Central
  central_n_unique = create_value_df(central$n_unique_people, "20000"),
  central_rank = create_value_df(central$rank, ""),
  central_pct_convicted_criminal = create_value_df(central$pct_convicted_criminal, "100%"),
  central_avg_detention_length = create_value_df(central$avg_detention_length, "20 Days"),
  central_over_60 = create_value_df(central$over_60, "20 Days"),
  central_countries = central$top_5_departure_countries,
  central_transfers = central$top_5_transfer_destinations,
  central_age = central$age_summary,
  central_ethnicity = central$ethnicity_summary,
  central_gender = central$gender_summary,
  
  # South
  south_n_unique = create_value_df(south$n_unique_people, "20000"),
  south_rank = create_value_df(south$rank, ""),
  south_pct_convicted_criminal = create_value_df(south$pct_convicted_criminal, "100%"),
  south_avg_detention_length = create_value_df(south$avg_detention_length, "20 Days"),
  south_over_60 = create_value_df(south$over_60, "20 Days"),
  south_countries = south$top_5_departure_countries,
  south_transfers = south$top_5_transfer_destinations,
  south_age = south$age_summary,
  south_ethnicity = south$ethnicity_summary,
  south_gender = south$gender_summary,
  
  # Pine Prairie
  pine_n_unique = create_value_df(pine$n_unique_people, "20000"),
  pine_rank = create_value_df(pine$rank, ""),
  pine_pct_convicted_criminal = create_value_df(pine$pct_convicted_criminal, "100%"),
  pine_avg_detention_length = create_value_df(pine$avg_detention_length, "20 Days"),
  pine_over_60 = create_value_df(pine$over_60, "20 Days"),
  pine_countries = pine$top_5_departure_countries,
  pine_transfers = pine$top_5_transfer_destinations,
  pine_age = pine$age_summary,
  pine_ethnicity = pine$ethnicity_summary,
  pine_gender = pine$gender_summary,
  
  # Allen Parish
  allen_n_unique = create_value_df(allen$n_unique_people, "20000"),
  allen_rank = create_value_df(allen$rank, ""),
  allen_pct_convicted_criminal = create_value_df(allen$pct_convicted_criminal, "100%"),
  allen_avg_detention_length = create_value_df(allen$avg_detention_length, "20 Days"),
  allen_over_60 = create_value_df(allen$over_60, "20 Days"),
  allen_countries = allen$top_5_departure_countries,
  allen_transfers = allen$top_5_transfer_destinations,
  allen_age = allen$age_summary,
  allen_ethnicity = allen$ethnicity_summary,
  allen_gender = allen$gender_summary,
  
  # Jackson Parish
  jackson_n_unique = create_value_df(jackson$n_unique_people, "20000"),
  jackson_rank = create_value_df(jackson$rank, ""),
  jackson_pct_convicted_criminal = create_value_df(jackson$pct_convicted_criminal, "100%"),
  jackson_avg_detention_length = create_value_df(jackson$avg_detention_length, "20 Days"),
  jackson_over_60 = create_value_df(jackson$over_60, "20 Days"),
  jackson_countries = jackson$top_5_departure_countries,
  jackson_transfers = jackson$top_5_transfer_destinations,
  jackson_age = jackson$age_summary,
  jackson_ethnicity = jackson$ethnicity_summary,
  jackson_gender = jackson$gender_summary,
  
  # Richwood
  richwood_n_unique = create_value_df(richwood$n_unique_people, "20000"),
  richwood_rank = create_value_df(richwood$rank, ""),
  richwood_pct_convicted_criminal = create_value_df(richwood$pct_convicted_criminal, "100%"),
  richwood_avg_detention_length = create_value_df(richwood$avg_detention_length, "20 Days"),
  richwood_over_60 = create_value_df(richwood$over_60, "20 Days"),
  richwood_countries = richwood$top_5_departure_countries,
  richwood_transfers = richwood$top_5_transfer_destinations,
  richwood_age = richwood$age_summary,
  richwood_ethnicity = richwood$ethnicity_summary,
  richwood_gender = richwood$gender_summary,
  
  # Winn
  winn_n_unique = create_value_df(winn$n_unique_people, "20000"),
  winn_rank = create_value_df(winn$rank, ""),
  winn_pct_convicted_criminal = create_value_df(winn$pct_convicted_criminal, "100%"),
  winn_avg_detention_length = create_value_df(winn$avg_detention_length, "20 Days"),
  winn_over_60 = create_value_df(winn$over_60, "20 Days"),
  winn_countries = winn$top_5_departure_countries,
  winn_transfers = winn$top_5_transfer_destinations,
  winn_age = winn$age_summary,
  winn_ethnicity = winn$ethnicity_summary,
  winn_gender = winn$gender_summary,
  
  # River
  river_n_unique = create_value_df(river$n_unique_people, "20000"),
  river_rank = create_value_df(river$rank, ""),
  river_pct_convicted_criminal = create_value_df(river$pct_convicted_criminal, "100%"),
  river_avg_detention_length = create_value_df(river$avg_detention_length, "20 Days"),
  river_over_60 = create_value_df(river$over_60, "20 Days"),
  river_countries = river$top_5_departure_countries,
  river_transfers = river$top_5_transfer_destinations,
  river_age = river$age_summary,
  river_ethnicity = river$ethnicity_summary,
  river_gender = river$gender_summary,
  
  # Louisiana statewide
  la_n_unique = create_value_df(la_n_unique, "20000"),
  la_rank = create_value_df(la_rank, ""),
  la_pct_convicted_criminal = create_value_df(la_pct_crim, "100%"),
  la_avg_detention_length = create_value_df(la_mean_length, "20 Days"),
  la_over_60 = create_value_df(la_over_60, "20 Days"),
  la_countries = la_countries,
  la_transfers = la_transfers,
  la_age = la_age,
  la_ethnicity = la_ethnicity,
  la_gender = la_gender
)

# Write each dataframe to its own sheet
sheet_id <- "1RAfEbT9Nq2JXhC6giKBPO34kpy0cHisAp8hmiLmThCA"

for (name in names(all_data)) {
  sheet_write(
    data = all_data[[name]],
    ss = sheet_id,
    sheet = name
  )
}
