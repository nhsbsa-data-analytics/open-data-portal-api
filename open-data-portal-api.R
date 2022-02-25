# 1. Script details ------------------------------------------------------------

# Name of script: OpenDataAPIQuery
# Description:  Using R to query the NHSBSA open data portal API. 
# Created by: Matthew Wilson (NHSBSA)
# Created on: 26-03-2020
# Latest update by: Adam Ivison (NHSBSA)
# Latest update on: 24-06-2021
# Update notes: Updated endpoint in the script, refactored code and added async

# R version: created in 3.5.3

# 2. Load packages -------------------------------------------------------------

# List packages we will use
packages <- c(
  "jsonlite", # 1.6
  "dplyr",    # 0.8.3
  "crul"      # 1.1.0
)

# Install packages if they aren't already
if (length(setdiff(packages, rownames(installed.packages()))) > 0) {
    install.packages(setdiff(packages, rownames(installed.packages())))  
}

# 3. Define variables ----------------------------------------------------------

# Define the url for the API call
base_endpoint <- "https://opendata.nhsbsa.net/api/3/action/"
package_list_method <- "package_list"     # List of data-sets in the portal
package_show_method <- "package_show?id=" # List all resources of a data-set
action_method <- "datastore_search_sql?"  # SQL action method

# Send API call to get list of data-sets
datasets_response <- jsonlite::fromJSON(paste0(
  base_endpoint, 
  package_list_method
))

# Now lets have a look at the data-sets currently available
datasets_response$result

# For this example we're interested in the English Prescribing Dataset (EPD).
# We know the name of this data-set so can set this manually, or access it 
# from datasets_response.
dataset_id <- "english-prescribing-data-epd"

# 4. API calls for single month ------------------------------------------------

# Define the parameters for the SQL query
resource_name <- "EPD_202001" # For EPD resources are named EPD_YYYYMM
pco_code <- "13T00" # Newcastle Gateshead CCG
bnf_chemical_substance <- "0407010H0" # Paracetamol

# Build SQL query (WHERE criteria should be enclosed in single quotes)  
single_month_query <- paste0(
  "
  SELECT 
      * 
  FROM `", 
      resource_name, "` 
  WHERE 
      1=1 
  AND pco_code = '", pco_code, "' 
  AND bnf_chemical_substance = '", bnf_chemical_substance, "'
  "
)

# Build API call
single_month_api_call <- paste0(
  base_endpoint,
  action_method,
  "resource_id=",
  resource_name, 
  "&",
  "sql=",
  URLencode(single_month_query) # Encode spaces in the url
)

# Grab the response JSON as a list
single_month_response <- jsonlite::fromJSON(single_month_api_call)

# Extract records in the response to a dataframe
single_month_df <- single_month_response$result$result$records

# Lets have a quick look at the data
str(single_month_df)
head(single_month_df)

# You can use any of the fields listed in the data-set within the SQL query as 
# part of the select or in the where clause in order to filter.

# Information on the fields present in a data-set and an accompanying data 
# dictionary can be found on the page for the relevant data-set on the Open Data 
# Portal.

# 5. API calls for data for multiple months ------------------------------------

# Now that you have extracted data for a single month, you may want to get the 
# data for several months, or a whole year.

# Firstly we need to get a list of all of the names and resource IDs for every 
# EPD file. We therefore extract the metadata for the EPD dataset.
metadata_repsonse <- jsonlite::fromJSON(paste0(
  base_endpoint, 
  package_show_method,
  dataset_id
))

# Resource names and IDs are kept within the resources table returned from the 
# package_show_method call.
resources_table <- metadata_repsonse$result$resources

# We only want data for one calendar year, to do this we need to look at the 
# name of the data-set to identify the year. For this example we're looking at 
# 2020.
resource_name_list <- resources_table$name[grepl("2020", resources_table$name)]

# 5.1. For loop ----------------------------------------------------------------

# We can do this with a for loop that makes all of the individual API calls for 
# you and combines the data together into one dataframe

# Initialise dataframe that data will be saved to
for_loop_df <- data.frame()

# As each individual month of EPD data is so large it will be unlikely that your 
# local system will have enough RAM to hold a full year's worth of data in 
# memory. Therefore we will only look at a single CCG and chemical substance as 
# we did previously

# Loop through resource_name_list and make call to API to extract data, then 
# bind each month together to make a single data-set
for(month in resource_name_list) {
  
  # Build temporary SQL query 
  tmp_query <- paste0(
    "
    SELECT 
        * 
    FROM `", 
        month, "` 
    WHERE 
        1=1 
    AND pco_code = '", pco_code, "' 
    AND bnf_chemical_substance = '", bnf_chemical_substance, "'
    "
  )
  
  # Build temporary API call
  tmp_api_call <- paste0(
    base_endpoint,
    action_method,
    "resource_id=",
    month, 
    "&",
    "sql=",
    URLencode(tmp_query) # Encode spaces in the url
  )
  
  # Grab the response JSON as a temporary list
  tmp_response <- jsonlite::fromJSON(tmp_api_call)
  
  # Extract records in the response to a temporary dataframe
  tmp_df <- tmp_response$result$result$records
  
  # Bind the temporary data to the main dataframe
  for_loop_df <- dplyr::bind_rows(for_loop_df, tmp_df)
}

# 5.2. Async -- ----------------------------------------------------------------

# We can call the API asynchronously and this will result in an approx 10x speed 
# increase over a for loop for large resource_names by vectorising our approach.

# Construct the SQL query as a function
async_query <- function(resource_name) {
  paste0(
    "
    SELECT 
        * 
    FROM `", 
        resource_name, "` 
    WHERE 
        1=1 
    AND pco_code = '", pco_code, "' 
    AND bnf_chemical_substance = '", bnf_chemical_substance, "'
    "
  )
}

# Create the API calls
async_api_calls <- lapply(
  X = resource_name_list,
  FUN = function(x) 
    paste0(
      base_endpoint,
      action_method,
      "resource_id=",
      x, 
      "&",
      "sql=",
      URLencode(async_query(x)) # Encode spaces in the url
    )
)

# Use crul::Async to get the results
dd <- crul::Async$new(urls = async_api_calls)
res <- dd$get()

# Check that everything is a success
all(vapply(res, function(z) z$success(), logical(1)))

# Parse the output into a list of dataframes
async_dfs <- lapply(
  X = res, 
  FUN = function(x) {
    
    # Parse the response
    tmp_response <- x$parse("UTF-8")
    
    # Extract the records
    tmp_df <- jsonlite::fromJSON(tmp_response)$result$result$records
  }
)

# Concatenate the results 
aysnc_df <- do.call(dplyr::bind_rows, async_dfs)

# 6. Export the data -----------------------------------------------------------

# Use write.csv for ease
write.csv(single_month_df, "single_month.csv")
write.csv(for_loop_df, "for_loop.csv")
write.csv(aysnc_df, "aysnc.csv")
