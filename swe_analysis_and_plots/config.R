# Definitions collected from
# data_functions.R
# main_driver.R
# plot_functions.R
# swe_data_and_plot.R

# From main_driver.R:
#dev_dir
#db_dir
#db_start_ymdh
#db_finish_ymdh
#csv_output_dir
#plot_output_dir
#fromDate
#toDate
#target_hour
#minus_hours
#plus_hours
#include_station_criteria
#bounding_box
#domain_text
#no_data_value
#verbose

# From swe_data_and_plot.R:
# Padding values used to calculate min_lat, max_lat, min_lon, max_lon

dev_dir <- "/net/home/fall/nwm_da/swe_analysis_and_plots/"
db_dir <- "/net/scratch/fall/m3db/yearly/"
db_start_ymdh <- "2019100100"
db_finish_ymdh <- "2020093023"
scratch_dir <- "/net/scratch/fall"
csv_output_dir <- "/net/scratch/fall/m3db/yearly/eval"
plot_output_dir <- "/net/scratch/fall/m3db/yearly/eval/images"
fromDate <- "2019-12-01 12:00:00"
toDate <- "2020-02-01 12:00:00"
target_hour <- 12
minus_hours <- 3
plus_hours <- 3
include_station_criteria <- 0.75
## bounding_box <- c(min_lat = 20.0, max_lat = 58.0,
##                   min_lon = -134.0, max_lon = -60.0)
bounding_box <- c(min_lat = 24.0, max_lat = 54.0,
                  min_lon = -128.0, max_lon = -66.0)
domain_text <- "conus"

