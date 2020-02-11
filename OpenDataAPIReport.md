---
title: "OpenDataAPIReport"
author: "Matthew Wilson"
output: 
  html_document:
    keep_md:  true
---



## Creating a report directly from NHSBSA Open Data

It is possible to pull in the data from the NHSBSA Open Data Portal and build a report around this using markdown.  
Firstly, lets library some packages and set some variables like the ```url``` we are going to be using for the API calls. We will need ```httr``` and ```jsonlite``` to query the API and parse the data that the query returns.


```r
library(dplyr)
library(tidyr)
library(ggplot2)
library(httr)
library(jsonlite)

url <- "https://opendata.nhsbsa.net"
```

We then want to query the API to give us a list of available datasets and resources. 

```r
path <- "/api/3/action/package_list"

query_raw_result <- GET(url = url, path = path)  

print(query_raw_result$status_code)
```

```
## [1] 200
```

We check the status code is 200 to make sure the query was succesful.
