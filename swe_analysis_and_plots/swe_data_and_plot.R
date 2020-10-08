#Query/Get data from sqlite databases and plot them
swe_data_ana_plot <- function(dev_dir,
                              db_dir, db_start_ymdh, db_finish_ymdh,
                              csv_output_dir,
                              plot_output_dir,
                              fromDate, toDate,
                              target_hour, hr_range,
                              bounding_box,
                              domain_text,
                              include_station_criteria=0,
                              no_data_value,
                              verbose) {
    
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
                                                    db_fromDate, db_toDate) 

    nwm_csv_ori_path <- file.path(data_path, nwm_swe_ori_csv_fname)
    wdb_swe_ori_csv_fname <- get_time_related_fname("wdb_swe_hourly_original_",
                                                    db_fromDate, db_toDate)

    wdb_csv_ori_path <- file.path(data_path, wdb_swe_ori_csv_fname)
    
    
    #Form extracted file paths --- either queried from database or
    # extracted from the original queried hourly data for the short period
    nwm_swe_extracted_csv_fname <- 
        get_time_related_fname("nwm_swe_hourly_extracted_",
                               fromDate, toDate, 
                               target_hour, hr_range)
    nwm_csv_extracted_path <- file.path(data_path, nwm_swe_extracted_csv_fname)
    
    wdb_swe_extracted_csv_fname <- 
        get_time_related_fname("wdb_swe_hourly_extracted_",
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
                                                      fromDate, toDate,
                                                      target_hour, hr_range)
    nwm_wdb_processed_path <- file.path(data_path, nwm_wdb_processed_fname)
    

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
                               fromDate, toDate, 
                               target_hour, hr_range)
    station_exclude_list_path <- file.path(csv_output_dir,
                                           station_exclude_csv_fname)
    if (file.exists(station_exclude_list_path)) {
        excluded_ids <- read.csv(station_exclude_list_path)
        `%notin%` <- Negate(`%in%`)
        nwm_wdb_com_daily <- nwm_wdb_com_daily %>% filter(station_id %notin% excluded_ids$station_id)
        
    } else {
        message("Info/Warning: Station exclusion file does not exist!")
    }
    
    #exc_id <- "PORQ1"
    
    
    #Further calculate statistics based on the processed nwm_com and wdb_com
    all_stats <- acc_abl_analysis(nwm_wdb_com_daily)
    # all_stats[[1]]  for accumulatiomn  -- contains departure, hit, and miss info
    # all_stats[[2]]  for ablation  -- contains departure, hit, and miss info
    # all_stats[[3]]  for persistent under ablation -- contains departure, hit, and miss info
    
    # 
    # #How to find the obj_id for stations that have the maximum obs_swe_diff_sum value
    # #abl_stats %>% filter(obs_swe_diff_sum==max(obs_swe_diff_sum))
    # exc_obj_id_abl <- (abl_stats %>% 
    #                        filter(obs_swe_diff_sum==max(obs_swe_diff_sum)))$obj_identifier
    # 
    # exc_obj_id_acc <- (acc_stats %>% 
    #                        filter(obs_swe_diff_sum==max(obs_swe_diff_sum)))$obj_identifier
    
    #***********************************************************
    
    #Plot resultes below *********************************************************
    
    ####### Some common info below #####################
    message("\nPlotting/Saving analysis result ...")
    library(ggmap)
    library(ggplot2)
    library(ggforce)
    #source("/nwcdev/nwm_da/m3_dev/plot_functions.R")
    source(paste0(dev_dir, "plot_functions.R"))
    
    ndays <- as.numeric(as.difftime(as.Date(toDate)-as.Date(fromDate)),
                        units="days")
    min_obs_ndays <- ceiling(include_station_criteria * ndays)
    if (include_station_criteria == 0) {
        include_perc_str <- paste0("_with_any_data")
    } else {
        include_perc_str <- paste0("_with", include_station_criteria * 100, "percent")
    }
    
    
    # #define boundaries for Western USA
    min_lat <- bounding_box[["min_lat"]] + 1  #30.0
    max_lat <- bounding_box[["max_lat"]] + 1  # 50.0
    min_lon <- bounding_box[["min_lon"]] + 0  # -125.0
    max_lon <- bounding_box[["max_lon"]] + 1  # -100.0
     
    myBox <- c(min_lon, min_lat,
               max_lon, max_lat)
    
    bg_map <- ggmap::get_map(location=myBox, source="stamen", maptype="terrain", crop=TRUE)
    # bg_map <- map_setup(wdb_com, min_lat, max_lat, min_lon, max_lon,
    #                     minLatAdj=1, maxLatAdj=1, minLonAdj=0, maxLonAdj=1)
    
    
    if (target_hour < 0) {
        
        post <- paste0(gsub('.{1}$', '', domain_text), include_perc_str, ".png")

    } else {
        
        post <- paste0("_", target_hour, "zm",
                       hr_range[1], "p", hr_range[2],
                       gsub('.{1}$', '', domain_text), include_perc_str, ".png")
        #Note: gsub('.{1}$', '', domain_text) -- get rid of the last character, here '_'
    }
    
    val_breaks <- c(-100, -2.0, -1.0, -0.6, -0.2, 0.2, 0.6, 1.0, 2.0, 100)
    val_breaks <- c(0.01, 0.1, 0.25, 0.5, 0.75, 1.25, 2.0, 4.0, 10.0, 100.0)
    val_breaks <- c(-Inf, -2.0, -1.0, -0.6, -0.2, 0.2, 0.6, 1.0, 2.0, Inf)
    color_breaks <- c('#BF8F60', '#CF004B', '#F67100', '#FFD817', '#E6FFE6',
                      '#17D8FF', '#0071F6', '#4B00F6', '#BF60BF')
    val_size_min_lim <- 0
    val_size_max_lim <- max(max(all_stats[[1]]$obs_swe_diff_sum, na.rm=TRUE),
                            max(all_stats[[2]]$obs_swe_diff_sum, na.rm=TRUE),
                            max(all_stats[[3]]$obs_swe_diff_sum, na.rm=TRUE))
    # val_size_max_lim <- max(max(acc_stats$obs_swe_diff_sum, na.rm=TRUE),
    #                         max(abl_stats$obs_swe_diff_sum, na.rm=TRUE),
    #                         max(abl_stats_pers$obs_swe_diff_sum, na.rm=TRUE))
    val_size_max_lim=1200 #NA #1000 #200
    min_thresh_colr <- 0
    max_thresh_colr <- NA #1000 #200
    histlim <- ceiling(max(nrow(all_stats[[1]]), nrow(all_stats[[2]]), nrow(all_stats[[3]]))/2)
    #histlim <- ceiling(max(nrow(acc_hit), nrow(abl_hit), nrow(abl_hit_pers))/2)
    histlim <- 500 #1000  #500
    alpha_val <- 0.8
    size_min_pt <- 0
    size_max_pt <- 10  #has been 10
    color_low <- "blue"
    color_mid <- "white"
    color_high <- "red"
    plot_dpi <- 300
    map_width <- 7
    map_height <- 7
    
    min_swe_sum_val <- 0 # 10.0  # total swe cutoff values
    min_sample_size_n <- 0 # 15  # for number of cutoff samples
    
    if (exists("target_hour") && target_hour >= 0) {
        plot_subtitle <- paste0(toupper(gsub("_", " ", domain_text)),
                                "[From ", fromDate, " To ", toDate, "] at hour ", target_hour, "z")
    } else {
        plot_subtitle <- paste0(toupper(gsub("_", " ", domain_text)),
                                "[From ", fromDate, " To ", toDate, "]")
        #Note: toupper(gsub("_", " ", domain_text)): replace _ with space and make them all upper case
    }
    ####### Some common info above #####################
    
    size_label="Sum Obs\nSWE Diff\n(mm)"
    
    #=========================================================
    # ACCUMULATION RELATED PLOTS
    #=========================================================
    
    # Aggregate Accumulation Departure  --- based on new matrix
    plot_title <- "Aggregate Accumulation Departure Map"
    
    data_sub <- all_stats[[1]]
    
    ggplot(acc_stats, aes(x=mean_obs_swe, y=departure)) + geom_point()
    ggplot(acc_stats, aes(x=mean_obs_swe, y=acc_hit_aggbias)) + geom_point()
    ggplot(acc_stats, aes(x=mean_obs_swe, y=acc_miss_aggerror)) + geom_point()
    
    # data_sub <- subset(acc_stats, subset = obs_sum >= min_swe_sum_val)
    
    if (min_sample_size_n > 0) data_sub <- data_sub %>% subset(num_events >= min_sample_size_n)
    if (min_obs_ndays > 0) data_sub <- data_sub %>% subset(obs_ndays >= min_obs_ndays)
    
    map_bar_plot <- plot_map_errors(bg_map, data_sub, xcoln="lon", ycoln="lat",
                                    size_var_coln="obs_swe_diff_sum", val_coln="departure",
                                    plot_title=plot_title, plot_subtitle=plot_subtitle,
                                    x_label="Longitude", y_label="Latitude",
                                    size_label=size_label,
                                    #color_label="Acc\nDeparture",
                                    color_label="ARCD",
                                    hist_title="Accumulation Departure",
                                    color_breaks,
                                    size_min_pt=size_min_pt,
                                    size_max_pt=size_max_pt,
                                    color_low=color_low,
                                    color_mid=color_mid, 
                                    color_high=color_high,
                                    val_size_min_lim=val_size_min_lim,
                                    val_size_max_lim=val_size_max_lim,
                                    min_thresh_colr=min_thresh_colr,
                                    max_thresh_colr=max_thresh_colr,
                                    val_breaks, alpha_val=alpha_val,
                                    histlim=histlim)
                                   
    mapPlotName <- formFileName("acc_departure_map_", fromDate, toDate, post)
    mapOutput_path <- file.path(plot_output_dir, mapPlotName)
    ggsave(filename=mapOutput_path, plot=map_bar_plot[[1]], units="in",
           width=map_width, height=map_height, dpi=plot_dpi)
    
    barPlotName <- formFileName("acc_departure_distribution_", fromDate, toDate, post)
    barOutput_path <- file.path(plot_output_dir, barPlotName)
    ggsave(filename=barOutput_path, plot=map_bar_plot[[2]], units="in",
           width=6, height=4, dpi=plot_dpi)

    #-----------------------------------------------------------
    # Aggregate Accumulation Bias  --- based on new matrix
    plot_title <- "Aggregate Accumulation Bias Map"

    map_bar_plot <- plot_map_errors(bg_map, data_sub, xcoln="lon", ycoln="lat",
                                    size_var_coln="obs_swe_diff_sum", val_coln="acc_hit_aggbias",
                                    plot_title=plot_title, plot_subtitle=plot_subtitle,
                                    x_label="Longitude", y_label="Latitude",
                                    size_label=size_label,
                                    color_label="ARCB",
                                    hist_title="Agg Acc Bias",
                                    color_breaks,
                                    size_min_pt=size_min_pt,
                                    size_max_pt=size_max_pt,
                                    color_low=color_low,
                                    color_mid=color_mid, 
                                    color_high=color_high,
                                    val_size_min_lim=val_size_min_lim,
                                    val_size_max_lim=val_size_max_lim,
                                    min_thresh_colr=min_thresh_colr,
                                    max_thresh_colr=max_thresh_colr,
                                    val_breaks, alpha_val=alpha_val,
                                    histlim=histlim)
                                         
    mapPlotName <- formFileName("acc_hit_aabias_map_", fromDate, toDate, post)
    barPlotName <- formFileName("acc_hit_aabias_distribution_", fromDate, toDate, post)
    mapOutput_path <- file.path(plot_output_dir, mapPlotName)
    barOutput_path <- file.path(plot_output_dir, barPlotName)
   
    ggsave(filename=mapOutput_path, plot=map_bar_plot[[1]], units="in",
           width=map_width, height=map_height, dpi=plot_dpi)
    
    ggsave(filename=barOutput_path, plot=map_bar_plot[[2]], units="in",
           width=6, height=4, dpi=plot_dpi)

    #
    #-----------------------------------------------------------
    # Aggregate Accumulation Bias  --- based on new matrix
    plot_title <- "Aggregate Accumulation Error Map"

    map_bar_plot <- plot_map_errors(bg_map, data_sub, xcoln="lon", ycoln="lat",
                                    size_var_coln="obs_swe_diff_sum", val_coln="acc_miss_aggerror",
                                    plot_title=plot_title, plot_subtitle=plot_subtitle,
                                    x_label="Longitude", y_label="Latitude",
                                    size_label=size_label,
                                    color_label="ARCE",
                                    hist_title="Agg Acc Error",
                                    color_breaks,
                                    size_min_pt=size_min_pt,
                                    size_max_pt=size_max_pt,
                                    color_low=color_low,
                                    color_mid=color_mid, 
                                    color_high=color_high,
                                    val_size_min_lim=val_size_min_lim,
                                    val_size_max_lim=val_size_max_lim,
                                    min_thresh_colr=min_thresh_colr,
                                    max_thresh_colr=max_thresh_colr,
                                    val_breaks, alpha_val=alpha_val,
                                    histlim=histlim)
                                           
    mapPlotName <- formFileName("acc_miss_aabias_map_", fromDate, toDate, post)
    barPlotName <- formFileName("acc_miss_aaerror_distribution_", fromDate, toDate, post)
    mapOutput_path <- file.path(plot_output_dir, mapPlotName)
    barOutput_path <- file.path(plot_output_dir, barPlotName)
    ggsave(filename=mapOutput_path, plot=map_bar_plot[[1]], units="in",
           width=map_width, height=map_height, dpi=plot_dpi)
    ggsave(filename=barOutput_path, plot=map_bar_plot[[2]], units="in",
           width=6, height=4, dpi=plot_dpi)
    
    #=========================================================
    # ABLATION RELATED PLOTS
    #=========================================================
    #-----------------------------------------------------------
    # Aggregate Ablation Departure  --- based on new matrix
    
    #data_sub <- abl_stats
    data_sub <- all_stats[[2]]
    if (min_sample_size_n > 0) data_sub <- data_sub %>% subset(num_events >= min_sample_size_n)
    if (min_obs_ndays > 0) data_sub <- data_sub %>% subset(obs_ndays >= min_obs_ndays)
    plot_title <- "Aggregate Ablation Departure Map"
    map_bar_plot <- plot_map_errors(bg_map, data_sub, xcoln="lon", ycoln="lat",
                                    size_var_coln="obs_swe_diff_sum", val_coln="departure",
                                    plot_title=plot_title, plot_subtitle=plot_subtitle,
                                    x_label="Longitude", y_label="Latitude",
                                    size_label=size_label,
                                    color_label="ARBD",
                                    hist_title="Ablation Departure",
                                    color_breaks,
                                    size_min_pt=size_min_pt,
                                    size_max_pt=size_max_pt,
                                    color_low=color_low,
                                    color_mid=color_mid, 
                                    color_high=color_high,
                                    val_size_min_lim=val_size_min_lim,
                                    val_size_max_lim=val_size_max_lim,
                                    min_thresh_colr=min_thresh_colr,
                                    max_thresh_colr=max_thresh_colr,
                                    val_breaks, alpha_val=alpha_val,
                                    histlim=histlim)
                                  
    
    mapPlotName <- formFileName("abl_departure_map_", fromDate, toDate, post)
    barPlotName <- formFileName("abl_departure_distribution_", fromDate, toDate, post)
    mapOutput_path <- file.path(plot_output_dir, mapPlotName)
    barOutput_path <- file.path(plot_output_dir, barPlotName)
    ggsave(filename=mapOutput_path, plot=map_bar_plot[[1]], units="in",
           width=map_width, height=map_height, dpi=plot_dpi)
    ggsave(filename=barOutput_path, plot=map_bar_plot[[2]], units="in",
           width=6, height=4, dpi=plot_dpi)

    #-----------------------------------------------------------
    # Aggregate Ablation Bias  --- based on new matrix
    plot_title <- "Aggregate Ablation Bias Map"

    map_bar_plot <- plot_map_errors(bg_map, data_sub, xcoln="lon", ycoln="lat",
                                    size_var_coln="obs_swe_diff_sum", val_coln="abl_hit_aggbias",
                                    plot_title=plot_title, plot_subtitle=plot_subtitle,
                                    x_label="Longitude", y_label="Latitude",
                                    size_label=size_label,
                                    color_label="ARBB",
                                    hist_title="Agg Abl Bias",
                                    color_breaks,
                                    size_min_pt=size_min_pt,
                                    size_max_pt=size_max_pt,
                                    color_low=color_low,
                                    color_mid=color_mid, 
                                    color_high=color_high,
                                    val_size_min_lim=val_size_min_lim,
                                    val_size_max_lim=val_size_max_lim,
                                    min_thresh_colr=min_thresh_colr,
                                    max_thresh_colr=max_thresh_colr,
                                    val_breaks, alpha_val=alpha_val,
                                    histlim=histlim)
                                         
    mapPlotName <- formFileName("abl_hit_arbb_map_", fromDate, toDate, post)
    barPlotName <- formFileName("abl_hit_arbb_distribution_", fromDate, toDate, post)
    mapOutput_path <- file.path(plot_output_dir, mapPlotName)
    barOutput_path <- file.path(plot_output_dir, barPlotName)
    ggsave(filename=mapOutput_path, plot=map_bar_plot[[1]], units="in",
           width=map_width, height=map_height, dpi=plot_dpi)
    ggsave(filename=barOutput_path, plot=map_bar_plot[[2]], units="in",
           width=6, height=4, dpi=plot_dpi)
    
    #-----------------------------------------------------------
    # Aggregate Ablation Bias  --- based on new matrix
    plot_title <- "Aggregate Ablation Error Map"

    map_bar_plot <- plot_map_errors(bg_map, data_sub, xcoln="lon", ycoln="lat",
                                    size_var_coln="obs_swe_diff_sum", val_coln="abl_miss_aggerror",
                                    plot_title=plot_title, plot_subtitle=plot_subtitle,
                                    x_label="Longitude", y_label="Latitude",
                                    size_label=size_label,
                                    color_label="ARBE",
                                    hist_title="Agg Abl Error",
                                    color_breaks,
                                    size_min_pt=size_min_pt,
                                    size_max_pt=size_max_pt,
                                    color_low=color_low,
                                    color_mid=color_mid, 
                                    color_high=color_high,
                                    val_size_min_lim=val_size_min_lim,
                                    val_size_max_lim=val_size_max_lim,
                                    min_thresh_colr=min_thresh_colr,
                                    max_thresh_colr=max_thresh_colr,
                                    val_breaks, alpha_val=alpha_val,
                                    histlim=histlim)
                                           
    mapPlotName <- formFileName("abl_miss_arbe_map_", fromDate, toDate, post)
    barPlotName <- formFileName("abl_miss_arbe_distribution_", fromDate, toDate, post)
    mapOutput_path <- file.path(plot_output_dir, mapPlotName)
    barOutput_path <- file.path(plot_output_dir, barPlotName)
    ggsave(filename=mapOutput_path, plot=map_bar_plot[[1]], units="in",
           width=map_width, height=map_height, dpi=plot_dpi)
    ggsave(filename=barOutput_path, plot=map_bar_plot[[2]], units="in",
           width=6, height=4, dpi=plot_dpi)
    
    
    #=========================================================
    # ABLAION WITH PERSISTENT CONDITION RELATED PLOTS
    #=========================================================
    
    #-----------------------------------------------------------
    # Aggregate Ablation Departure  --- under persistent condition
    plot_title <- "Aggregate Ablation Departure Map Under Persistent Condition"
    data_sub <- all_stats[[3]]
    if (min_sample_size_n > 0) data_sub <- data_sub %>% subset(num_events >= min_sample_size_n)
    if (min_obs_ndays > 0) data_sub <- data_sub %>% subset(obs_ndays >= min_obs_ndays)
    map_bar_plot <- plot_map_errors(bg_map, data_sub, xcoln="lon", ycoln="lat",
                                    size_var_coln="obs_swe_diff_sum", val_coln="departure",
                                    plot_title=plot_title, plot_subtitle=plot_subtitle,
                                    x_label="Longitude", y_label="Latitude",
                                    size_label=size_label,
                                    color_label="ARBD",
                                    hist_title="Ablation Departure (Persistent)",
                                    color_breaks,
                                    size_min_pt=size_min_pt,
                                    size_max_pt=size_max_pt,
                                    color_low=color_low,
                                    color_mid=color_mid, 
                                    color_high=color_high,
                                    val_size_min_lim=val_size_min_lim,
                                    val_size_max_lim=val_size_max_lim,
                                    min_thresh_colr=min_thresh_colr,
                                    max_thresh_colr=max_thresh_colr,
                                    val_breaks, alpha_val=alpha_val,
                                    histlim=histlim)
                                  
    mapPlotName <- formFileName("abl_departure_map_pers_", fromDate, toDate, post)
    barPlotName <- formFileName("abl_departure_distribution_pers_", fromDate, toDate, post)
    
    mapOutput_path <- file.path(plot_output_dir, mapPlotName)
    barOutput_path <- file.path(plot_output_dir, barPlotName)
    ggsave(filename=mapOutput_path, plot=map_bar_plot[[1]], units="in",
           width=map_width, height=map_height, dpi=plot_dpi)
    ggsave(filename=barOutput_path, plot=map_bar_plot[[2]], units="in",
           width=6, height=4, dpi=plot_dpi)
    
    #-----------------------------------------------------------
    # Aggregate Ablation with persistent condition Bias  --- based on new matrix
    plot_title <- "Aggregate Ablation Bias Map Under Persistent Condition"
    map_bar_plot <- plot_map_errors(bg_map, data_sub, xcoln="lon", ycoln="lat",
                                    size_var_coln="obs_swe_diff_sum", val_coln="abl_hit_aggbias",
                                    plot_title=plot_title, plot_subtitle=plot_subtitle,
                                    x_label="Longitude", y_label="Latitude",
                                    size_label=size_label,
                                    color_label="ARBB",
                                    hist_title="Agg Abl Bias (Persistent)",
                                    color_breaks,
                                    size_min_pt=size_min_pt,
                                    size_max_pt=size_max_pt,
                                    color_low=color_low,
                                    color_mid=color_mid, 
                                    color_high=color_high,
                                    val_size_min_lim=val_size_min_lim,
                                    val_size_max_lim=val_size_max_lim,
                                    min_thresh_colr=min_thresh_colr,
                                    max_thresh_colr=max_thresh_colr,
                                    val_breaks, alpha_val=alpha_val,
                                    histlim=histlim)
                                         
    mapPlotName <- formFileName("abl_hit_arbb_map_pers_", fromDate, toDate, post)
    barPlotName <- formFileName("abl_hit_arbb_distribution_pers_", fromDate, toDate, post)
 
    mapOutput_path <- file.path(plot_output_dir, mapPlotName)
    barOutput_path <- file.path(plot_output_dir, barPlotName)
    ggsave(filename=mapOutput_path, plot=map_bar_plot[[1]], units="in",
           width=map_width, height=map_height, dpi=plot_dpi)
    ggsave(filename=barOutput_path, plot=map_bar_plot[[2]], units="in",
           width=6, height=4, dpi=plot_dpi)
    
    #-----------------------------------------------------------
    # Aggregate Ablation Error with Persistent Condition  --- based on new matrix
    plot_title <- "Aggregate Ablation Error Map Under Persistent Condition"

    map_bar_plot <- plot_map_errors(bg_map, data_sub, xcoln="lon", ycoln="lat",
                                    size_var_coln="obs_swe_diff_sum", val_coln="abl_miss_aggerror",
                                    plot_title=plot_title, plot_subtitle=plot_subtitle,
                                    x_label="Longitude", y_label="Latitude",
                                    size_label=size_label,
                                    color_label="ARBE",
                                    hist_title="Agg Abl Error (Persistent)",
                                    color_breaks,
                                    size_min_pt=size_min_pt,
                                    size_max_pt=size_max_pt,
                                    color_low=color_low,
                                    color_mid=color_mid, 
                                    color_high=color_high,
                                    val_size_min_lim=val_size_min_lim,
                                    val_size_max_lim=val_size_max_lim,
                                    min_thresh_colr=min_thresh_colr,
                                    max_thresh_colr=max_thresh_colr,
                                    val_breaks, alpha_val=alpha_val,
                                    histlim=histlim)
                                           
    mapPlotName <- formFileName("abl_miss_arbe_map_pers_", fromDate, toDate, post)
    barPlotName <- formFileName("abl_miss_arbe_distribution_pers_", fromDate, toDate, post)

    mapOutput_path <- file.path(plot_output_dir, mapPlotName)
    barOutput_path <- file.path(plot_output_dir, barPlotName)
    ggsave(filename=mapOutput_path, plot=map_bar_plot[[1]], units="in",
           width=map_width, height=map_height, dpi=plot_dpi)
    ggsave(filename=barOutput_path, plot=map_bar_plot[[2]], units="in",
           width=6, height=4, dpi=plot_dpi)
    #-----------------------------------------------------------
    message("Done\n")
    
    #Return resulting dataframe for debugging purpose
    return (all_stats)
}

