#
# Connect to spark and do some analyses
# Jasper Ginn
# 30/09/2016
#

# Download phoenix data if data folder does not exist OR if there are no files in the data folder.
source(paste0(getwd(), "/functions/downloadPhoenixData.R"))
if(!dir.exists(paste0(getwd(), "/data/")) | length(list.files(paste0(getwd(), "/data/"))) == 0) {
  cat("Downloading phoenix data files to /data/ folder.")
  downloadPhoenixData()
} else{
  cat("Events are already downloaded. Moving on ...")
}



# Connect to Spark
library(sparklyr)
library(dplyr)
library(ggplot2)
Sys.setenv(SPARK_HOME="/usr/lib/spark")
config <- spark_config()
sc <- spark_connect(master = "yarn-client", config = config, version = '1.6.2')

# Cache flights Hive table into Spark
tbl_cache(sc, 'flights')
flights_tbl <- tbl(sc, 'flights')

# Get number of rows
spark_disconnect(sc)
