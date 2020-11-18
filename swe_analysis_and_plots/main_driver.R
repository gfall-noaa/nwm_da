#!/usr/bin/env Rscript

#The main driver to call other functions to conduct data analysis and plotting
#
#--------------------------------------------------------------------
# TO Run:
#        $ Rscript --vanilla main_driver.R <configuration_file_name>
#
#    Note: If configuration_file_name is not given, it uses config.R which
#          defines actual configuration file to be used.
#--------------------------------------------------------------------

#Check where this code will be run and provide a configuration file needed

args = commandArgs(trailingOnly=TRUE)
#if (startup:::is_rstudio_console()) {
if ("tools:rstudio" %in% search()) {
   source("config.R")
   message("sourced config.R for rstudio")
} else if (length(args)==0) {
   ifelse (file.exists("config.R"),
           source("config.R"),
           stop("Error: config.R does not exist."))

   message("sourecd default config.R")
} else {
   fileName <- paste0(args[1])
   ifelse (file.exists(fileName),
           source(fileName),
           stop("Error: config file ", fileName, " does not exist."))
   message("sourced given config file: ", fileName)
}

#Get login info
#username <- Sys.getenv("LOGNAME")

no_data_value <- -99999.0
verbose = NA


hr_range <- c(minus_hours, plus_hours)

ifelse (dir.exists(dev_dir),
       source(paste0(dev_dir, "get_swe_stats.R")),
       message("Incorrect dev_dir path or file name"))
if (!dir.exists(db_dir)) {
   stop("Incorrect database directory:  ", db_dir)
   #Within rstudio: Code -> Source, it will stop at the first stop statement
   # Or just click the Source button above the code window
}
if (!dir.exists(csv_output_dir)) {
   stop("CSV output directory does not exist:  ", csv_output_dir)
}
if (!dir.exists(plot_output_dir)) {
   stop("Plots output directory does not exist:  ", plot_output_dir)
}

if (!dir.exists(scratch_dir)) {
    stop("scratch directory does not exist:  ", scratch_dir)
}                                       #
# #station_exclude_list_file <- paste0(csv_output_dir, "/station_exclude_list.csv")
# if (!file.exists(station_exclude_list_path)) {
#     stop("Station excluding list file does not exist:  ", station_exclude_list_path)
# }

all_stats <- get_swe_stats(dev_dir,
                           db_dir, db_start_ymdh, db_finish_ymdh,
                           csv_output_dir,
                           scratch_dir,
                           fromDate, toDate,
                           target_hour, hr_range,
                           domain_text,
                           include_station_criteria,
                           no_data_value,
                           verbose)
message("SWE stats are ready")

source(paste0(dev_dir, "plot_swe_stats.R"))
plot_swe_stats(all_stats,
               dev_dir,
               plot_output_dir,
               fromDate, toDate,
               target_hour, hr_range,
               bounding_box,
               domain_text,
               include_station_criteria)

message("Done with the plotting")

acc_stats <- all_stats[[1]]
abl_stats <- all_stats[[2]]
abl_pers_stats <- all_stats[[3]]
no_obs_diff_stats <- all_stats[[4]]
rm(all_stats)

message("All Done")
