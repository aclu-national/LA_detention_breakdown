# Louisiana Detention Breakdown (2025)

## Overview
This project processes and analyzes 2025 U.S. Immigration and Customs Enforcement (ICE) detention data to generate statistical profiles for detention facilities in Louisiana. This tool provides data-driven transparency into the demographics, detention duration, and movement patterns of individuals detained within the state's immigration system.

## Project Objectives
The analysis aims to illuminate key metrics including:
* **Operational Volume:** Analyzing total bookings.
* **Demographics:** Providing accurate age, gender, and ethnicity breakdowns.
* **Confinement Patterns:** Tracking average detention lengths and prolonged confinement (>60 days).

## Methodology & Logic

### 1. Unique Headcounts
The analysis calculates the total number of **unique people**  detained in a facility, rather than the number of administrative bookings.

### 2. Demographic Deduplication
Demographic breakdowns (Age, Gender, Ethnicity) count unique individuals within each specific category.
* **Logic:** An individual detained multiple times is counted only once per category to prevent skewing the data.
* **Status Changes:** In rare instances where an individual's status changes between bookings (e.g., aging into a new bracket), they are counted once in *each* applicable category.

### 3. Operational Metrics
Metrics related to facility operations, such as **Average Detention Length** and **Conviction Status**, utilize the full dataset of all bookings (including re-admissions).

## Data Sources
* **Detention Data:** Processed 2025 detention stints from the [Deportation Data Project](https://github.com/deportationdata/ice).
* **Facility Metadata:** Facility names and locations derived from the [Vera Institute of Justice](https://github.com/vera-institute/ice-detention-trends).
