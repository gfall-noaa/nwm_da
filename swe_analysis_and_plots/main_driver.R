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
if (!dir.exists(db_dir)) {
    stop("Incorrect database directory:  ", db_dir)
    #Within rstudio: Code -> Source, it will stop at the first stop statement
    # Or just click the Source button above the soirce window
}
#db_dir <- "/net/scratch/fall/m3db/yearly"
db_start_ymdh <- "2019100100"
db_finish_ymdh <- "2020053123"

#give directory where output csv files will be written
csv_output_dir <- paste0("/disks/scratch/", username, "/m3db/csv_output")
if (!dir.exists(csv_output_dir)) {
    stop("CSV output directory does not exist:  ", csv_output_dir)
}
plot_output_dir <- paste0("/net/home/", username, "/dev/nwm_da/swe_analysis_and_plots/pngs")
if (!dir.exists(plot_output_dir)) {
    stop("Plots output directory does not exist:  ", plot_output_dir)
}


#Manually give the analysis period.
fromDate <- "2019-12-01 12:00:00"
#toDate <- "2019-11-30 12:00:00"
fromDate <- "2020-04-01 12:00:00"
toDate <- "2020-05-31 12:00:00"


target_hour <- 12 # Set to a nagetive or comment out will process every hour of data
minus_hours <- 3 # smaller than the target hour
plus_hours <- 3  # number of hours greater than the target hour
hr_range <- c(minus_hours, plus_hours)

include_station_criteria <- 0.
#(must have 0.5*100 = 50% record during the analysis period)

bounding_box <- c(min_lat = 20.0, max_lat = 58.0, 
                  min_lon = -134.0, max_lon = -60.0)  #CONUS
domain_text <- "_conus_"
bounding_box <- c(min_lat = 30.0, max_lat = 50.0, 
                  min_lon = -125.0, max_lon = -100.0)  #Western USA
domain_text <- "_western_usa_"
#bounding_box <- c(0.0, 50.0, -125.0, -100.0)


no_data_value <- -99999.0
verbose = NA

# 
# #station_exclude_list_file <- paste0(csv_output_dir, "/station_exclude_list.csv")
# if (!file.exists(station_exclude_list_path)) {
#     stop("Station excluding list file does not exist:  ", station_exclude_list_path)
# }

#swe_data_ana_plot(dev_dir,
all_stats <- swe_data_ana_plot(dev_dir,
                  db_dir, db_start_ymdh, db_finish_ymdh,
                  csv_output_dir,
                  plot_output_dir,
                  fromDate, toDate,
                  target_hour, hr_range,
                  bounding_box,
                  domain_text,
                  include_station_criteria,
                  no_data_value,
                  verbose)
acc_stats <- all_stats[[1]]
abl_stats <- all_stats[[2]]
abl_pers_stats <- all_stats[[3]]
no_obs_diff_stats <- all_stats[[4]]
rm(all_stats)
message("All Done")