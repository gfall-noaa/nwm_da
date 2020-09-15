#The main driver to call other functions to conduct data analysis and ploting

#Get login info
username <- Sys.getenv("LOGNAME")

#Give the directory where the source code and functions are located
#dev_dir <- "/net/home/zzhang/dev/nwm_da/swe_analysis_and_plots/"
dev_dir <- paste0("/net/home/", username, "/dev/nwm_da/swe_analysis_and_plots/")

ifelse (dir.exists(dev_dir), 
        source(paste0(dev_dir, "swe_data_and_plot.R")),
        message("Incorrect dev_dir path or file name"))

#Give the directory where the m3 databases are located
db_dir <- "/net/scratch/zzhang/m3db/western_us/"  #for Western USA case
#db_dir <- "/net/scratch/fall/m3db/yearly"
db_start_ymdh <- "2019100100"
db_finish_ymdh <- "2020053123"

#give directory where output csv files will be written
csv_output_dir <- paste0("/disks/scratch/", username, "/m3db/csv_output")
plot_output_dir <- paste0("/net/home/", username, "/dev/nwm_da/swe_analysis_and_plots/pngs")

station_exclude_list_file <- paste0(csv_output_dir, "/station_exclude_list.csv")
#Manually give the analysis period.
#fromDate <- "2019-12-01 12:00:00"
# toDate <- "2019-11-30 12:00:00"
fromDate <- "2020-03-01 12:00:00"
toDate <- "2020-05-31 12:00:00"


target_hour <- 12 # Set to a nagetive or comment out will process every hour of data
minus_hours <- 3 # smaller than the target hour
plus_hours <- 3  # number of hours greater than the target hour
hr_range <- c(minus_hours, plus_hours)

bounding_box <- c(min_lat = 20.0, max_lat = 58.0, 
                  min_lon = -134.0, max_lon = -60.0)  #CONUS
bounding_box <- c(min_lat = 30.0, max_lat = 50.0, 
                  min_lon = -125.0, max_lon = -100.0)  #Western USA
#bounding_box <- c(0.0, 50.0, -125.0, -100.0)
domain_text <- '_conus_'
domain_text <- '_western_usa_'
no_data_value <- -99999.0
verbose = NA

swe_data_ana_plot(dev_dir,
                  db_dir, db_start_ymdh, db_finish_ymdh,
                  csv_output_dir,
                  plot_output_dir,
                  station_exclude_list_file,
                  fromDate, toDate,
                  target_hour, hr_range,
                  bounding_box,
                  no_data_value,
                  verbose)

message("All Done")