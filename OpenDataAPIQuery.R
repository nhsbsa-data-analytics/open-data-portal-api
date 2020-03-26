# 1. Script details ----------------------------------------------------------
# Name of script: OpenDataAPIQuery
# Description:  Using R to query the NHSBSA open data portal API. 
# Created by: Matthew Wilson
# Created on: 26-03-2020
# Latest update by:
# Latest update on:
# Update notes:
# 
# R version: created in 3.5.3

# 2. Load packages --------------------------------------------------------
library(dplyr) #V 0.8.3
library(data.table) #V 1.12.8
library(httr) #V 1.4.1
library(jsonlite) # 1.6

# 3. Define variables -----------------------------------------------------

## set url to be used for API calls
url <- "https://opendata.nhsbsa.net"

## use 'package_list' API call to get list of all datasets held within portal
package_list_path <- "/api/3/action/package_list"

datasets <- fromJSON(paste0(url, package_list_path))

datasets$result

## for this example we're interested in the PDPI dataset.
## We know the name of this dataset so can set this manually, or access it 
## from the datasets object.
# TODO: confirm dataset_id
# dataset_id <- "primary-care-prescribing-and-dispensing"

# 4. API calls for single month -------------------------------------------

## At first we just want to pull out a single full month of data. These datasets
## can be 4GB or larger, therefore we have to be careful when pulling them into
## local memory. Depending on your system configuration this could cause issues.
month_resource_id <- "1071e8e7-e146-4d98-9283-57ac4a3402d4"

## as the default for a standard call to the API places a limit on the number of records
## returned we're going to use a SQL query to pull in the data.

## The below query will pull in all data that is held in the datastore for the 
## listed resource ID.
# month_all_data_query <- URLencode(paste0("select * from \"", month_resource_id, "\" where 1 = 1"))

## here we build the full API call
# month_all_data_api_call <- paste0(url,
#                                   "/api/3/action/datastore_search_sql?sql=",
#                                   month_all_data_query)

## the API call is then sent to the API and returns a list object containing the data
## and some other things such as the SQL used to create the data
# month_all_data_result <- fromJSON(month_all_data_api_call)

## we're just interested in the data so pull that out into a standalone dataframe
# month_all_data_df <- month_all_data_result$result$records

## we can also alter our SQL query to filter on a specific field, such as primary care organisation
## or BNF code. Search criteria should be enclosed in single qutoes
search_pco_code <- "'13T00'" # in our example we will use the code for Newcastle Gateshead CCG
search_bnf_code <- "'0407010H0AAAMAM'" # and the BNF code for Paracetamol 500mg tablets

## build SQL query.  
month_query <- URLencode(paste0("select * from \"", month_resource_id, 
                                "\" where 1 = 1",
                                " and \"PCO_CODE\" = ", search_pco_code,
                                " and \"BNF_CODE\" = ", search_bnf_code))

## build API call
month_api_call <- paste0(url,
                         "/api/3/action/datastore_search_sql?sql=",
                         month_query)

## send API call
month_result <- fromJSON(month_api_call)

## here we extract the data into it's own standalone dataframe
month_df <- month_result$result$records

## lets have a quick look at the data
str(month_df)
View(head(month_df))

## The SQL that we use in the query can be modified in many ways to return only what you want or need
## and can be treated as any other SQL query. We can get a list of all available fields by
## running a quick query and checking the fields object that is returned
## build sql query, returning the first row of the dataset only
fields_query <- URLencode(paste0("select * from \"", month_resource_id, 
                                 "\" where 1 = 1 fetch first 1 row only"))

## build API call
fields_api_call <- paste0(url,
                          "/api/3/action/datastore_search_sql?sql=",
                          fields_query)

## send API call
fields_result <- fromJSON(fields_api_call)

## extract list of fields into it's own dataframe
fields_df <- fields_result$result$fields

View(fields_df)

## you can use any of the fields listed in the dataset within the SQL query
## as part of the select or in the where clause in order to filter.

# 5. API calls for data for multiple months -------------------------------

## Now that you have extracted data for a single month, you may want to get the data for several
## months, or a whole year. We can do this with a for loop that makes all of
## the individual API calls for you and combines the data together into one dataframe

## Firstly we need to get a list of all of the names and resource IDs for every 
## PDPI file. We therefore extract the metadata for the PDPI dataset.
metadata <- fromJSON(paste0(url,"/api/3/action/package_show?id=",dataset_id))

## resource names and IDs are kept within the resources table returned from
## the package_show call.
resources_table <- metadata$result$resources

## We only want data for one calendar year, to do this we need to look at the name
## of the dataset to identify the year. For this example we're looking at 2019
resources <- resources_table %>%
  filter(grepl("2019", name))

## Initialise dataframe that data will be saved to
pdpi_df <- data.frame()

## as each individual month of PDPI data is so large it will be unlikely
## that your local system will have enough RAM to hold a full year's
## worth of data in memory. Therefore we will only look at a single 
## chemical substance, Atorvastatin - 0212000B0
search_chem_sub_code <- "'0212000B0'"

## Loop through ID list and make call to API to extract data, then bind each
## month together to make a single dataset
for(i in seq_along(resources$id)) {
  ## we're going to use a SQL query to get the data that we want
  loop_query <- URLencode(paste0("select * from \"", resources$id[i], 
                                 "\" where 1 = 1",
                                 " and \"BNF_CHEMICAL_SUBSTANCE\" = ", search_chem_sub_code))
  
  ## build temporary API call
  loop_api_call <- paste0(url,
                          "/api/3/action/datastore_search_sql?sql=",
                          loop_query)
  
  ## send API call
  loop_result <- fromJSON(loop_api_call)
  
  ## extract records into temporary standalone dataframe
  loop_df <- loop_result$result$records
  
  ## union the temporary data to the main pdpi dataframe
  pdpi_df <- bind_rows(pdpi_df, loop_df)
}


# 6. Exporting the data ---------------------------------------------------

## now that we have extracted the data that we are interested, we can export it
## we'll use the fwrite() function in the data.table package to export the 
## data, as it is many times quicker than the base write.csv() function.

fwrite(month_all_data_df, "full-month-data.csv")
fwrite(month_df, "api-data.csv")
fwrite(pdpi_df, "pdpi-data.csv")

## end of script ##
##########################################################################################
url <- paste0("https://opendata.nhsbsa.net/api/3/action/",
              "datastore_search?",
              "resource_id=1071e8e7-e146-4d98-9283-57ac4a3402d4")
page <- GET(url) # API request

df <- fromJSON(rawToChar(page$content))

status_code(page) # return status code



for(i in seq_along(resources_table$id)) {
  ## need to find the total records in dataset as query automatically limits result
  total_records <- fromJSON(paste0(url,
                            "/api/3/action/datastore_search?id=",
                            resources_table$id[i],
                            "&limit=1"))$result$total
  
  ## query API again, but this time for all data
  result <- fromJSON(paste0(url,
                            "/api/3/action/datastore_search?id=",
                            resources_table$id[i],
                            "&limit=",
                            total_records))
  
  ## pass data to temporary table to allow to be appended to main data frame
  tmp_df <- result$result$records
  
  ## append queried data to main data frame
  pdpi_data <- bind_rows(pdpi_data, tmp_df)
}

end_time <- Sys.time()

end_time - start_time



test_query <- URLencode(paste0("select * from \"", month_resource_id, 
                                "\" where 1 = 1",
                                
                                " and \"BNF_CHEMICAL_SUBSTANCE\" = ", search_chem_sub_code))

test_api_call <- paste0(url,
                        "/api/3/action/datastore_search_sql?sql=",
                        test_query)

test_results <- fromJSON(test_api_call)

test_df <- test_results$result$records

View(test_df)

raw <- fromJSON(paste0(url,"/api/3/action/package_show?id=",datasets$result[37]))

resources <- raw$result$resources$id

data_list <- list()

data_list <- fromJSON(paste0(url,"/api/3/action/datastore_search_sql?id=3b6f7e12-f204-4f61-9e31-53837bf92a10"))

data_list$result$fields
data_list$result$records
nrow(data_list$result$records)
data_list$result$total

totaldf <- fromJSON(paste0(url,"/api/3/action/datastore_search?resource_id=3b6f7e12-f204-4f61-9e31-53837bf92a10&limit=1"))
