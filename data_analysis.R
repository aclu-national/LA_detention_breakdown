# Loading libraries
library(tidyverse)
library(janitor)
library(readxl)
library(openssl)
library(data.table)
library(clipr)
library(forcats)

# Reading facility lookup
facility_lookup <- read_csv("facilities.csv") %>% 
  clean_names()

# Defining 2024 data link
link_2024 <- "https://ucla.app.box.com/index.php?rm=box_download_shared_file&shared_name=9d8qnnduhus4bd5mwqt7l95kz34fic2v&file_id=f_1922078517539"

# Defining function to read Excel
read_detention_data <- function(link, skip_rows) {
  tmpfile <- tempfile(fileext = ".xlsx")
  download.file(link, destfile = tmpfile, mode = "wb")
  df <- read_excel(tmpfile, skip = skip_rows) %>% clean_names()
  unlink(tmpfile)
  return(df)
}

# Downloading and cleaning 2024 data
data_2024 <- read_detention_data(link_2024, skip_rows = 6)

data_clean <- data_2024 %>%
  left_join(facility_lookup, by = "detention_facility_code") %>%
  mutate(
    stay_in = as.Date(stay_book_in_date_time),
    stay_out = as.Date(stay_book_out_date),
    detention_in = as.Date(book_in_date_time),
    detention_out = as.Date(detention_book_out_date_time),
    stay_length = as.numeric(difftime(stay_out, stay_in, units = "days")),
    detention_length = as.numeric(difftime(detention_out, detention_in, units = "days")),
    stay_in_year = year(stay_in),
    stay_out_year = year(stay_out),
    detention_in_year = year(detention_in),
    detention_out_year = year(detention_out),
    stay_in_quarter_year = paste0(year(stay_in), " Q", quarter(stay_in)),
    stay_out_quarter_year = paste0(year(stay_out), " Q", quarter(stay_out)),
    detention_in_quarter_year = paste0(year(detention_in), " Q", quarter(detention_in)),
    detention_out_quarter_year = paste0(year(detention_out), " Q", quarter(detention_out)),
    stay_in_age_low = if_else(!is.na(stay_in_year) & !is.na(birth_year), stay_in_year - birth_year, NA_integer_),
    stay_in_age_high = stay_in_age_low - 1,
    stay_out_age_low = if_else(!is.na(stay_out_year) & !is.na(birth_year), stay_out_year - birth_year, NA_integer_),
    stay_out_age_high = stay_out_age_low - 1,
    detention_in_age_low = if_else(!is.na(detention_in_year) & !is.na(birth_year), detention_in_year - birth_year, NA_integer_),
    detention_in_age_high = detention_in_age_low - 1,
    detention_out_age_low = if_else(!is.na(detention_out_year) & !is.na(birth_year), detention_out_year - birth_year, NA_integer_),
    detention_out_age_high = detention_out_age_low - 1,
    LA_vs_all = if_else(state == "LA", "LA", "Other"),
    age_category = case_when(
      detention_in_age_low < 20 ~ "< 20",
      detention_in_age_low < 40 ~ "20 - 40",
      detention_in_age_low < 60 ~ "40 - 60",
      detention_in_age_low >= 60 ~ "> 60",
      TRUE ~ NA_character_
    )
  ) %>%
  filter(detention_in >= as.Date("2024-01-01") & detention_in < as.Date("2025-01-01"))

# Defining a transfer dataframe
transfer <- data_clean %>%
  filter(!is.na(unique_identifier)) %>%
  mutate(stayid = md5(paste0(stay_book_in_date_time, unique_identifier))) %>%
  group_by(stay_book_in_date_time, book_in_date_time, unique_identifier) %>%
  mutate(count = n()) %>%
  filter(count == 1) %>%
  select(-count) %>%
  ungroup() %>%
  group_by(stayid) %>%
  arrange(book_in_date_time) %>%
  mutate(
    detention_count = rleid(book_in_date_time),
    n_detentions = max(detention_count)
  ) %>%
  ungroup() %>%
  arrange(stayid, book_in_date_time) %>%
  mutate(
    moved = case_when(
      lead(detention_count) > detention_count & lead(stayid) == stayid ~ "Yes",
      is.na(detention_book_out_date_time) ~ "Active",
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
    stayid,
    stay_book_in_date_time,
    n_detentions,
    unique_identifier,
    book_in_date_time,
    detention_book_out_date_time,
    detention_facility_code,
    detention_facility,
    detention_release_reason,
    detention_count,
    state,
    moved,
    move_type,
    move_location
  )

# Creating a facility summarization function
summarize_facility_data <- function(facility_name) {
  facility_data <- filter(data_clean, detention_facility == facility_name)
  
  n_unique <- n_distinct(facility_data$unique_identifier)
  
  rank <- data_clean %>%
    count(detention_facility, name = "n") %>%
    arrange(desc(n)) %>%
    mutate(rank = row_number()) %>%
    filter(detention_facility == facility_name) %>%
    pull(rank)
  
  total_records <- nrow(facility_data)
  pct_crim <- round(
    ifelse(total_records > 0,
           sum(facility_data$book_in_criminality != "1 Convicted Criminal", na.rm = TRUE) / total_records * 100,
           NA_real_), 2
  )
  
  mean_length <- round(mean(facility_data$detention_length, na.rm = TRUE), 2)
  over_60 <- nrow(filter(facility_data, detention_length > 60))
  
  departure_country_summary <- facility_data %>%
    filter(!is.na(departure_country)) %>%
    count(departure_country, name = "n") %>%
    arrange(desc(n)) %>%
    mutate(departure_country = str_to_title(departure_country)) %>%
    slice_head(n = 5)
  
  transferred_cases <- transfer %>%
    filter(detention_facility == facility_name) %>%
    count(move_location, name = "n") %>%
    rename(detention_facility_code = move_location) %>%
    left_join(facility_lookup, by = "detention_facility_code") %>%
    filter(!is.na(detention_facility_name)) %>%
    arrange(desc(n)) %>%
    slice_head(n = 5) %>%
    select(detention_facility_name, n)
  
  age_summary <- facility_data %>%
    mutate(age_category = factor(age_category, levels = c("< 20", "20 - 40", "40 - 60", "> 60"))) %>%
    tabyl(age_category) %>%
    select(-percent)
  
  ethnicity_summary <- facility_data %>%
    count(ethnicity, name = "n") %>%
    mutate(ethnicity = case_when(
      ethnicity == "Hispanic Origin" ~ "Hispanic",
      ethnicity == "Not of Hispanic Origin" ~ "Not Hispanic",
      ethnicity == "Unknown" ~ "Unknown",
      TRUE ~ "None Reported"
    ))
  
  gender_summary <- facility_data %>%
    count(gender, name = "n")
  
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

# Running function on Louisiana detention facilities
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
# Creating data
la_data <- filter(data_clean, state == "LA")
la_transfer <- filter(transfer, state == "LA")

# Counting Unique
n_unique <- n_distinct(la_data$unique_identifier)

# Counting LA Rank
la_rank <- data_clean %>%
  count(state, name = "n") %>%
  arrange(desc(n)) %>%
  mutate(rank = row_number()) %>%
  filter(state == "LA")

# Counting percent not convicted criminal
total_records <- nrow(la_data)
pct_crim <- sum(la_data$book_in_criminality != "1 Convicted Criminal", na.rm = TRUE) / total_records

# Mean detention length
mean_detention_length <- mean(la_data$detention_length, na.rm = TRUE)

# Number of people detained > 60 days
over_60 <- nrow(filter(la_data, detention_length > 60))

# Country breakdown
la_data %>%
  count(departure_country, name = "n") %>%
  filter(!is.na(departure_country)) %>%
  arrange(desc(n)) %>%
  mutate(departure_country = str_to_title(departure_country)) %>%
  slice_head(n = 5)

# Transfer state breakdown
la_transfer %>%
  count(move_location, name = "facility_count") %>%
  rename(detention_facility_code = move_location) %>%
  left_join(facility_lookup, by = "detention_facility_code") %>%
  filter(!is.na(detention_facility_name)) %>%
  count(state, wt = facility_count, name = "total_transfers") %>%
  filter(state != "LA") %>%
  arrange(desc(total_transfers)) %>%
  slice_head(n = 5)

# Age breakdown
la_data %>% tabyl(age_category)

# Ethnicity breakdown
la_data %>%
  count(ethnicity, name = "n")

# Gender breakdown
la_data %>%
  count(gender, name = "n")
