#Query/Get data from sqlite databases or from csv files if available
get_swe_stats <- function(dev_dir,
                          db_dir, db_start_ymdh, db_finish_ymdh,
                          csv_output_dir,
                          scratch_dir,
                          fromDate, toDate,
                          target_hour, hr_range,
                          domain_text,
                          include_station_criteria=0,
                          no_data_value,
                          verbose) {
 
    #swe_data_processed <- process_swe_data()
    
    library(dplyr)
    library(lubridate) # In order to process date/time related data
    library(gsubfn)  # In order to use list[a, b] <- functionReturningTwoValues()
    
    #Give the directory where the source code and functions are located
    dev_dir <- dev_dir
    ifelse (dir.exists(dev_dir), 
            source(paste0(dev_dir, "data_functions.R")),
            stop("Error: Incorrect dev_dir path or file name"))
    
    nwm_base_db_name <- paste0("nwm_ana_station_neighbor_archive_",
                               db_start_ymdh, "_to_", 
                               db_finish_ymdh, "_base.db")
    ifelse (file.exists(paste0(db_dir, nwm_base_db_name)),
            nwm_base_db_path <- file.path(db_dir, nwm_base_db_name),
            stop("Error: Incorrect database path or file name."))
    #End of database definiation ------------------------------------
    
    #give directory where output csv files will be written
    csv_output_dir <- csv_output_dir
    ifelse (dir.exists(csv_output_dir),
            data_path <- csv_output_dir,
            stop("Error: Incorrect csv_output_dir given."))

    fromDate_ymdh <- date_string_to_date_ymdh(fromDate)
    toDate_ymdh <- date_string_to_date_ymdh(toDate)
    
    db_fromDate <- date_period(db_start_ymdh)
    db_toDate <- date_period(db_finish_ymdh)
    
    if ((!exists("fromDate") && !exists("toDate")) ||
        (fromDate==db_fromDate && toDate==db_toDate)) {
        fromDate <- db_fromDate
        toDate <- db_toDate
        subset_data <- FALSE
    } else {
        subset_data <- TRUE
    }
    
    
    #Get nwm and wdb swe csv file names for the analysis time period
    #There will be two types of csv files:
    #  1. Original queried without further process. The names of these
    #     files should contain the word 'original', time period, target_hour,
    #     and the hr_range info.
    #  2. Extracted: Extracted for specified target_hour and hr_range. Same stations
    #  3. Processed. They will have one value for each day and both NWM and OBS
    #     now have common dates and the same station info.
    
    nwm_swe_ori_csv_fname <- get_time_related_fname("nwm_swe_hourly_original_",
                                                    domain_text,
                                                    db_fromDate, db_toDate) 

    nwm_csv_ori_path <- file.path(data_path, nwm_swe_ori_csv_fname)
    wdb_swe_ori_csv_fname <- get_time_related_fname("wdb_swe_hourly_original_",
                                                    domain_text,
                                                    db_fromDate, db_toDate)

    wdb_csv_ori_path <- file.path(data_path, wdb_swe_ori_csv_fname)
    
    
    #Form extracted file paths --- either queried from database or
    # extracted from the original queried hourly data for the short period
    nwm_swe_extracted_csv_fname <- 
        get_time_related_fname("nwm_swe_hourly_extracted_",
                               domain_text,
                               fromDate, toDate, 
                               target_hour, hr_range)
    nwm_csv_extracted_path <- file.path(data_path, nwm_swe_extracted_csv_fname)
    
    wdb_swe_extracted_csv_fname <- 
        get_time_related_fname("wdb_swe_hourly_extracted_",
                               domain_text,
                               fromDate, toDate,
                               target_hour, hr_range)
    wdb_csv_extracted_path <- file.path(data_path, wdb_swe_extracted_csv_fname)
    
    # #Below will decide how to get the data: nwm_swe and wdb_swe
    # 
    # #Pre-determine file names for nwm and wdb data that have been processed
    # # according to the defined period, target_hour, and hr_range 
    # # --> (daily swe data extracted)
    # 
    nwm_wdb_processed_fname <- get_time_related_fname("nwm_wdb_daily_processed_",
                                                      domain_text,
                                                      fromDate, toDate,
                                                      target_hour, hr_range)
    nwm_wdb_processed_path <- file.path(data_path, nwm_wdb_processed_fname)
    # message(nwm_wdb_processed_path)
    # stop("checking..")
    

    if (file.exists(nwm_wdb_processed_path)) {
        
        message("Reading processed nwm/wdb daily data as csv a file ...")
        nwm_wdb_com_daily <- read.csv(nwm_wdb_processed_path)
        
    } else {
        
        if (file.exists(nwm_csv_extracted_path) & file.exists(wdb_csv_extracted_path)) {
            
            message("Reading extracted nwm/wdb daily data as csv files ...")
            nwm_swe <- read.csv(nwm_csv_extracted_path)
            wdb_swe <- read.csv(wdb_csv_extracted_path)
            
        } else if (file.exists(nwm_csv_ori_path) & file.exists(wdb_csv_ori_path)) {
            message("Original queried hourly data files exist. Read and extract for the new period.")
            # If the queried data for whole database period exist
            list[nwm_swe, wdb_swe] <- 
                subset_short_for_period(db_toDate, db_toDate,
                                        fromDate, toDate,
                                        nwm_csv_ori_path, wdb_csv_ori_path,
                                        target_hour, hr_range)
            message("Data was sucessfully extracted from the csv files.")
            write.csv(nwm_swe, nwm_csv_extracted_path, row.names = FALSE)
            write.csv(wdb_swe, wdb_csv_extracted_path, row.names = FALSE)
            
        } else {
            # get data from python modules if there is no csv files available 
            # For now, this only depends on if nwm/wdb_swe_csv_fname are given
            message("Getting the hourly data from the databases via python functions ...")

            list[nwm_swe, wdb_swe] <- get_data_via_py(nwm_base_db_path,
                                                      fromDate, 
                                                      toDate,
                                                      no_data_value,
                                                      bounding_box,
                                                      target_hour,
                                                      hr_range,
                                                      scratch_dir,
                                                      verbose=NA)  
            
            message("Python function call was successful")
            
            write.csv(wdb_swe, wdb_csv_extracted_path, row.names = FALSE)
            write.csv(nwm_swe, nwm_csv_extracted_path, row.names = FALSE)
            
            #now no need to subset
            subset_data <- FALSE
            
        }
        
        
        #Now the nwm_swe/wdb_swe are ready either from reading the csv or queried from database
        nwm_swe$datetime <- strptime(as.character(nwm_swe$datetime),
                                     format="%Y-%m-%d %H:%M:%S")
        wdb_swe$datetime <- strptime(as.character(wdb_swe$datetime),
                                     format="%Y-%m-%d %H:%M:%S")
        
        #Get data for those stations that exist in both datasets (but the length may different)
        nwm_in_wdb <- nwm_swe[nwm_swe$obj_identifier %in% wdb_swe$obj_identifier,]
        wdb_in_nwm <- wdb_swe[wdb_swe$obj_identifier %in% nwm_swe$obj_identifier,]
        
        #Note: May be need to convert datetime as as.character here if they are in POSIXct/POSIXlt
        #nwm_in_wdb$datetime <- as.character(nwm_in_wdb$datetime)
        #wdb_in_nwm$datetime <- as.character(wdb_in_nwm$datetime)
        #Now no need to convert because we need them as characters and they have not been converted 
        #  to POSIXct/POSIXlt
        
        #Convert datetime back to character
        nwm_in_wdb$datetime <- as.character(nwm_in_wdb$datetime)
        wdb_in_nwm$datetime <- as.character(wdb_in_nwm$datetime)
        
        #There are three options to calculate bias (return nwm_in_wdb and wdb_in_nwm):
        # 1) Based on all available hourly data for defined period
        # 2) Based on data sampled at the hour (target_hour) only
        # 3) Based on data sampled at the hour (target_hour). If the data at target_hour
        #   is missing,  search for the data at the nearest hour within the range hr_range
        # Note: It depends on hour target_hour and hr_range were given (negatives for no)
        
        nwm_wdb_com_daily <- sample_nwm_wdb_data(nwm_in_wdb, wdb_in_nwm, target_hour, hr_range)
        nwm_wdb_com_daily <- nwm_wdb_com_daily %>% arrange(obj_identifier)
        write.csv(nwm_wdb_com_daily, nwm_wdb_processed_path, row.names = FALSE)
       
    }
    
    #Get rid of data for stations in the excluding file
    # Station exclusion list file:
    station_exclude_csv_fname <- 
        get_time_related_fname("station_exclude_list_",
                               domain_text,
                               fromDate, toDate, 
                               target_hour, hr_range)
    station_exclude_list_path <- file.path(csv_output_dir,
                                           station_exclude_csv_fname)
    if (any(file.exists(station_exclude_list_path))) {
        excluded_ids <- read.csv(station_exclude_list_path)
        `%notin%` <- Negate(`%in%`)
        nwm_wdb_com_daily <- nwm_wdb_com_daily %>% filter(station_id %notin% excluded_ids$station_id)
        
    } else {
        message("Info/Warning: Station exclusion file does not exist!")
        message(paste0("The name should be ", station_exclude_csv_fname))
    }
    
    #exc_id <- "PORQ1"
    
    
    #Further calculate statistics based on the processed nwm_com and wdb_com
    all_stats <- acc_abl_analysis(nwm_wdb_com_daily)
    # all_stats[[1]]  for accumulatiomn  -- contains departure, hit, and miss info
    # all_stats[[2]]  for ablation  -- contains departure, hit, and miss info
    # all_stats[[3]]  for persistent under ablation -- contains departure, hit, and miss info
    

    #-----------------------------------------------------------
    #message("Done with getting SWE stats\n")

    return (all_stats)
}

