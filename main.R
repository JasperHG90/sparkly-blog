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

# You should now copy the files stored in the /data/ folder to hdfs and create a hive table so 
# That spark can access them. 

# Connect to spark
library(tidyverse)
library(sparklyr)
library(lubridate)
library(ggplot2)
library(phoxy)

# Settings for spark
Sys.setenv(SPARK_HOME="/usr/lib/spark")
config <- spark_config()
# Connect to spark on master node
sc <- spark_connect(master = "yarn-client", config = config, version = '1.6.2')

# Cache phoenix hive table into memory
tbl_cache(sc, 'phoenix')

# Add a proper date to the dataset
phoenix_tbl <- tbl(sc, "phoenix") %>%
  # Create a date from year/month/day combination
  mutate(datestamp = as.date(
    paste0(year, "-",
           ifelse(month < 10, paste0("0", month), month), "-",
           day)
  )) %>%
  # Filter for dates that are outside of scope (e.g. anything before june 2014)
  filter(datestamp >= "2014-06-22")

# Sum by date and plot
dplot <- phoenix_tbl %>%
  # Group by date
  group_by(datestamp) %>% 
  # Sum by date
  tally() %>%
  # Arrange in descending order
  arrange(datestamp) %>%
  # Collect to R
  collect() %>%
  # Plot
  ggplot(., aes(x=as.Date(ymd(datestamp)), y=n)) +
    geom_line() +
    theme_bw() +
    scale_x_date(name = "Date") +
    scale_y_continuous(name = "Number of Events") +
    geom_smooth()

# View
dplot

# Until feb 2015: frequently no events (0). This is annoying. Also a whole just before july no events.
# Filter data between feb 15 and 1st of june
phoenix_tbl <- phoenix_tbl %>%
  # Filter for dates
  filter(datestamp >= "2015-02-01" & datestamp <= "2016-06-01")

# Calculate the average goldstein score for each day and plot
avg.goldstein <- phoenix_tbl %>%
  # Group by day
  group_by(datestamp) %>%
  # For each day, calculate the average goldstein score
  summarize(avg_goldstein = mean(goldsteinscore)) %>%
  # Arrange by date
  arrange(datestamp) %>%
  # Collect
  collect() %>%
  # Plot
  ggplot(., aes(x=as.Date(ymd(datestamp)), y=avg_goldstein)) +
    geom_line() +
    theme_bw() +
    scale_x_date(name = "Date") +
    scale_y_continuous(name = "Average goldstein score") 

# Plot
avg.goldstein +
  geom_hline(yintercept = mean(avg.goldstein$data$avg_goldstein) + 2*sd(avg.goldstein$data$avg_goldstein), color="red") +
  geom_hline(yintercept = mean(avg.goldstein$data$avg_goldstein) - 2*sd(avg.goldstein$data$avg_goldstein), color="red") 

# Which dates are exceptionally negative?
negativeDays <- avg.goldstein$data %>% 
  filter(avg_goldstein < (mean(avg_goldstein) - 2*sd(avg_goldstein))) %>%
  arrange(avg_goldstein)

# Show mentions of USA, RUS, CHN and GBR
mentions <- phoenix_tbl %>%
  # Filter for countries
  filter(countrycode %in% c("USA", "RUS", "CHN", "GBR")) %>%
  # Group by date and countrycode 
  group_by(datestamp, countrycode) %>%
  # Tally
  tally() %>%
  # Arrange by date
  arrange(datestamp) %>%
  # Group by date
  group_by(datestamp) %>%
  # Normalize per day
  mutate(norm = n / sum(n)) %>%
  # Collect
  collect() %>%
  # Plot
  ggplot(., aes(x=as.Date(ymd(datestamp)), y=norm, group=countrycode, color=countrycode)) +
    geom_line() +
    theme_bw() +
    scale_x_date(name="Date") +
    scale_y_continuous(name="Percentage of events",
                       limits=c(0,1))

# Show plot
mentions

# Plot map of violent events in Syria.
# See codebook: https://s3.amazonaws.com/oeda/docs/phoenix_codebook.pdf

# Function to plot map
plotMap <- function(data, map) {
  map + 
    geom_point(data=data, aes(x=lon, y=lat, size=n), 
                col="red", alpha=0.4) +
    theme_bw()
}

# Get map of Syria
library(ggmap)
syr = as.numeric(geocode("Syria"))
syrmap = ggmap(get_googlemap(center=syr, scale=2, zoom=7), extent="normal")

# Get lat/lon of violent events in syria
syr.events <- phoenix_tbl %>%
  # Filter where countrycode == SYR & pentaclass == 4
  filter(countrycode == "SYR",
         pentaclass == 4) %>%
  # Group by lat / lon combination & count
  group_by(lat,lon) %>%
  tally() %>%
  arrange(desc(n)) %>%
  # Collect
  collect() %>%
  # Plot
  plotMap(., syrmap)

# Show
syr.events

# disconnect from spark
spark_disconnect(sc)

