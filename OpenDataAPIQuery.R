# 1. Script details ----------------------------------------------------------
# Name of script: OpenDataAPIQuery
# Description:  Using R to query the NHSBSA open data portal API. 
# Created by: Matthew Wilson (NHSBSA)
# Created on: 26-03-2020
# Latest update by:
# Latest update on:
# Update notes:
# 
# R version: created in 3.5.3

# 2. Load packages --------------------------------------------------------

## list of packages that we will be using
packages <- c("dplyr", "data table", "httr", "jsonlite")

## install packages if they aren't already
if (length(setdiff(packages, rownames(installed.packages()))) > 0) {
    install.packages(setdiff(packages, rownames(installed.packages())))  
}

## library packages
library(dplyr) #V 0.8.3
library(data.table) #V 1.12.8
library(httr) #V 1.4.1
library(jsonlite) # 1.6

## clear working enviroment
rm(list=ls()) 

# 3. Define variables -----------------------------------------------------

## set url to be used for API calls
url <- "https://opendata.nhsbsa.net"

## use 'package_list' API call to get list of all datasets held within portal
package_list_path <- "/api/3/action/package_list"

## send API call to get list of datasets
datasets <- fromJSON(paste0(url, package_list_path))

## now lets have a look at the datasets currently available
datasets$result

## for this example we're interested in the English Prescribing Dataset (EPD).
## We know the name of this dataset so can set this manually, or access it 
## from the datasets object.
dataset_id <- "english-prescribing-data-epd"

# 4. API calls for single month -------------------------------------------

#########################################
# At the moment the functionality to pull out a whole month of EPD data via the API
# with a single call is not enabled. This will be added in the near future.
# If you require a full month of data this can still be downloaded in CSV
# format direct from the Open Data Portal at https://opendata.nhsbsa.net/dataset/english-prescribing-data-epd
# we have provided example code below to indicate how a full month of data
# can be pulled out once the functionality is enabled, it is commented out currently.

# ## At first we just want to pull out a single full month of data. These datasets
# ## can be 4GB or larger, therefore we have to be careful when pulling them into
# ## local memory. Depending on your system configuration this could cause issues.
# month_resource_name <- "EPD_202001"

# ## as the default for a standard call to the API places a limit on the number of records
# ## returned we're going to use a SQL query to pull in the data.
# 
# ## The below query will pull in all data that is held in the datastore for the 
# ## listed resource ID.
# month_all_data_query <- URLencode(paste0("select * from ", month_resource_name," where 1 = 1"))
# 
# ## here we build the full API call
# month_all_data_api_call <- paste0(url,
#                                   "/api/3/action/datastore_search_sql?sql=",
#                                   month_all_data_query)
# 
# ## the API call is then sent to the API and returns a list object containing the data
# ## and some other things such as the SQL used to create the data
# month_all_data_result <- fromJSON(month_all_data_api_call)
# 
# ## we're just interested in the data so pull that out into a standalone dataframe
# month_all_data_df <- month_all_data_result$result$records

#########################################

## here we set the name of the resource that we want to access. For the English Prescribing Dataset (EPD)
## all resources are named in the format EPD_YYYYMM. Lets look at the latest available at the time
## of writing
month_resource_name <- "EPD_202001"

## we can also alter our SQL query to filter on a specific field, such as primary care organisation
## or BNF code. Search criteria should be enclosed in single qutoes
search_pco_code <- "'13T00'" # in our example we will use the code for Newcastle Gateshead CCG
search_bnf_code <- "'0407010H0AAAMAM'" # and the BNF code for Paracetamol 500mg tablets

## build SQL query.  
month_query <- URLencode(paste0("select * from ", month_resource_name, 
                                " where 1 = 1",
                                " and PCO_CODE = ", search_pco_code,
                                " and BNF_CODE = ", search_bnf_code))

## build API call
month_api_call <- paste0(url,
                         "/api/3/action/datastore_search_sql?sql=",
                         month_query)

## send API call
month_result <- fromJSON(month_api_call)

## here we extract the data into it's own standalone dataframe
month_df <- month_result$result$result$records

## lets have a quick look at the data
str(month_df)
View(head(month_df))

## you can use any of the fields listed in the dataset within the SQL query
## as part of the select or in the where clause in order to filter.

## information on the fields present in a dataset and an accompanying data dictionary
## can be found on the page for the relevant dataset on the Open Data Portal.

# 5. API calls for data for multiple months -------------------------------

## Now that you have extracted data for a single month, you may want to get the data for several
## months, or a whole year. We can do this with a for loop that makes all of
## the individual API calls for you and combines the data together into one dataframe

## Firstly we need to get a list of all of the names and resource IDs for every 
## EPD file. We therefore extract the metadata for the EPD dataset.
metadata <- fromJSON(paste0(url,"/api/3/action/package_show?id=",dataset_id))

## resource names and IDs are kept within the resources table returned from
## the package_show call.
resources_table <- metadata$result$resources

## We only want data for one calendar year, to do this we need to look at the name
## of the dataset to identify the year. For this example we're looking at 2019
resources <- resources_table %>%
  filter(grepl("2019", name))

## Initialise dataframe that data will be saved to
epd_df <- data.frame()

## as each individual month of EPD data is so large it will be unlikely
## that your local system will have enough RAM to hold a full year's
## worth of data in memory. Therefore we will only look at a single 
## chemical substance, Atorvastatin - 0212000B0
search_chem_sub_code <- "'0212000B0'"

## Loop through ID list and make call to API to extract data, then bind each
## month together to make a single dataset
for(i in seq_along(resources$name)) {
  ## we're going to use a SQL query to get the data that we want
  loop_query <- URLencode(paste0("select * from ", resources$name[i], 
                                 " where 1 = 1",
                                 " and BNF_CHEMICAL_SUBSTANCE = ", search_chem_sub_code))
  
  ## build temporary API call
  loop_api_call <- paste0(url,
                          "/api/3/action/datastore_search_sql?sql=",
                          loop_query)
  
  ## send API call
  loop_result <- fromJSON(loop_api_call)
  
  ## extract records into temporary standalone dataframe
  loop_df <- loop_result$result$result$records
  
  ## union the temporary data to the main pdpi dataframe
  epd_df <- bind_rows(epd_df, loop_df)
}


# 6. Exporting the data ---------------------------------------------------

## now that we have extracted the data that we are interested, we can export it
## we'll use the fwrite() function in the data.table package to export the 
## data, as it is many times quicker than the base write.csv() function. 
## the below functions will output the data to your default working directory
## or to a location you have set your working directory previously,

# fwrite(month_all_data_df, "full-month-data.csv")
fwrite(month_df, "api-data.csv")
fwrite(epd_df, "epd-data.csv")

