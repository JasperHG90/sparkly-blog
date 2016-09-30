#
# Download phoenix data and unzip.
# Jasper Ginn
# 30/09/2016
#

library(RCurl)
library(lubridate)

downloadPhoenixData <- function(dates = seq.Date(as.Date(ymd("2014-06-20")), 
                                                 as.Date(ymd("2016-09-29")), "days")) {
  
  
  # Create urls
  urls <- paste0("https://s3.amazonaws.com/oeda/data/current/events.full.",
                 as.character(stringr::str_replace_all(dates, "-", "")),
                 ".txt.zip")
  
  # Data folder
  df <- paste0(getwd(), "/data/")
  # Check if exists; else create
  if(!dir.exists(df)) dir.create(paste0(getwd(), "/data"))
  
  # Create filenames
  fn <- paste0(df, as.character(stringr::str_replace_all(dates, "-", "")), ".txt.zip")
  
  # Download & unzip
  for(i in 1:length(urls)) {
    # Download file
    download.file(urls[i], fn[i], method="curl")
    # Unzip
    unzip(fn[i], exdir=df)
    # Remove zip file
    io <- file.remove(fn[i])
  }
  
  cat("Downloaded files to /data/ directory.")
  
}