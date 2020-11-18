#Plot SWE statitics for accumulation, ablation,
#     and ablation with persistent snow cases within all_stats dataframe
plot_swe_stats <- function(all_stats,
                           dev_dir,
                           plot_output_dir,
                           fromDate, toDate,
                           target_hour, hr_range,
                           bounding_box,
                           domain_text,
                           include_station_criteria=0) {


    library(dplyr)
    library(lubridate) # In order to process date/time related data
    library(gsubfn)  # In order to use list[a, b] <- functionReturningTwoValues()

    ####### Some common info below #####################
    message("\nPlotting/Saving analysis result ...")
    library(ggmap)
    library(ggplot2)
    #library(ggforce)
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


    # # #define boundaries for Western USA
    # min_lat <- bounding_box[["min_lat"]] + 1  #30.0
    # max_lat <- bounding_box[["max_lat"]] + 1  # 50.0
    # min_lon <- bounding_box[["min_lon"]] + 0  # -125.0
    # max_lon <- bounding_box[["max_lon"]] + 1  # -100.0
    min_lat <- bounding_box[["min_lat"]]
    max_lat <- bounding_box[["max_lat"]]
    min_lon <- bounding_box[["min_lon"]]
    max_lon <- bounding_box[["max_lon"]]

    myBox <- c(min_lon, min_lat,
               max_lon, max_lat)

    bg_map <- ggmap::get_map(location=myBox, source="stamen", maptype="terrain", crop=TRUE)
    # bg_map <- map_setup(wdb_com, min_lat, max_lat, min_lon, max_lon,
    #                     minLatAdj=1, maxLatAdj=1, minLonAdj=0, maxLonAdj=1)


    if (target_hour < 0) {

        post <- paste0("_", domain_text, include_perc_str, ".png")
        #post <- paste0("_", gsub('.{1}$', '', domain_text), include_perc_str, ".png")

    } else {

        post <- paste0("_", target_hour, "zm",
                       hr_range[1], "p", hr_range[2], "_",
                       domain_text, include_perc_str, ".png")
                       # gsub('.{1}$', '', domain_text), include_perc_str, ".png")
        #Note: gsub('.{1}$', '', domain_text) -- get rid of the last character, here '_'
    }

    val_breaks <- c(-100, -2.0, -1.0, -0.6, -0.2, 0.2, 0.6, 1.0, 2.0, 100)
    val_breaks <- c(0.01, 0.1, 0.25, 0.5, 0.75, 1.25, 2.0, 4.0, 10.0, 100.0)
    val_breaks <- c(-Inf, -2.0, -1.0, -0.6, -0.2, 0.2, 0.6, 1.0, 2.0, Inf)
    color_breaks <- c('#BF8F60', '#CF004B', '#F67100', '#FFD817', '#E6FFE6',
                      '#17D8FF', '#0071F6', '#4B00F6', '#BF60BF')
    #below val_breaks and color_breaks are new from Greg
    val_breaks  <- c(-Inf, -1.5, -0.75, -0.25, 0.0, 0.25, 0.75, 1.5, Inf)
    val_breaks  <- c(-Inf, -1.0, -0.5, -0.25, 0.0, 0.25, 0.5, 1.0, Inf)
    val_breaks  <- c(-Inf, -1.4, -0.6, -0.2, 0.0, 0.2, 0.6, 1.4, Inf)
    val_breaks  <- c(-Inf, -2.0, -1.2, -0.6, -0.2, 0.0, 0.2, 0.6, 1.2, 2.0, Inf)
    color_breaks <- c('#BF8F60', '#CF004B', '#F67100', '#FFD817',
                      '#17D8FF', '#0071F6', '#4B00F6', '#BF60BF')
    color_breaks <- c('#BF8F60', '#CF004B', '#F67100', '#FFD817', '#FFFFBF',
                      '#BFFFFF', '#17D8FF', '#0071F6', '#4B00F6', '#BF60BF')
    color_breaks <- c('#BF8F60', '#CF004B', '#F67100', '#FFD817', '#FFFFA6',
                      '#A6FFFF', '#17D8FF', '#0071F6', '#4B00F6', '#BF60BF')
    color_breaks <- c('#BF8F60', '#CF004B', '#F67100', '#FFD817', '#FFF799',
                      '#BFFFFF', '#17D8FF', '#0071F6', '#4B00F6', '#BF60BF')
    val_size_min_lim <- 0
    val_size_max_lim <- max(max(all_stats[[1]]$obs_swe_diff_sum, na.rm=TRUE),
                            max(all_stats[[2]]$obs_swe_diff_sum, na.rm=TRUE),
                            max(all_stats[[3]]$obs_swe_diff_sum, na.rm=TRUE))
    # val_size_max_lim <- max(max(acc_stats$obs_swe_diff_sum, na.rm=TRUE),
    #                         max(abl_stats$obs_swe_diff_sum, na.rm=TRUE),
    #                         max(abl_stats_pers$obs_swe_diff_sum, na.rm=TRUE))
    val_size_max_lim=1200 #NA #1200 1000 #200
    min_thresh_colr <- 0
    max_thresh_colr <- NA #1000 #200
    histlim <- ceiling(max(nrow(all_stats[[1]]), nrow(all_stats[[2]]), nrow(all_stats[[3]]))/2)
    #histlim <- ceiling(max(nrow(acc_hit), nrow(abl_hit), nrow(abl_hit_pers))/2)
    histlim <- 70 #1000  #500
    alpha_val <- 1.0  #0.8  #when is 1.0, no transparency, shows real color
    size_min_pt <- 1.0  #0.0, 0.6
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
                                ", ", fromDate %>% substring(1,10),
                                " to ", toDate %>% substring(1,10),
                                " (at hour ", target_hour, "Z)")
    } else {
        plot_subtitle <- paste0(toupper(gsub("_", " ", domain_text)),
                                ", ", fromDate %>% substring(1,10),
                                " to ", toDate %>% substring(1,10))
        #Note: toupper(gsub("_", " ", domain_text)): replace _ with space and make them all upper case
    }
    ####### Some common info above #####################

    size_label="Total\nObserved\nAccum.\n(mm)"

    #=========================================================
    # ACCUMULATION RELATED PLOTS
    #=========================================================

    # Aggregate Accumulation Departure  --- based on new matrix
    plot_title <- "Aggregate Accumulation Departure"

    data_sub <- all_stats[[1]]

    ## scatter plots
    # ggplot(acc_stats, aes(x=mean_obs_swe, y=departure)) + geom_point()
    # ggplot(acc_stats, aes(x=mean_obs_swe, y=acc_hit_aggbias)) + geom_point()
    # ggplot(acc_stats, aes(x=mean_obs_swe, y=acc_miss_aggerror)) + geom_point()

    # data_sub <- subset(acc_stats, subset = obs_sum >= min_swe_sum_val)

    if (min_sample_size_n > 0) data_sub <- data_sub %>% subset(num_events >= min_sample_size_n)
    if (min_obs_ndays > 0) data_sub <- data_sub %>% subset(obs_ndays >= min_obs_ndays)
    data_sub <- data_sub %>% arrange(desc(obs_swe_diff_sum))  #So plot smaller circles on top

    map_bar_plot <- plot_map_errors(bg_map, data_sub, xcoln="lon", ycoln="lat",
                                    size_var_coln="obs_swe_diff_sum", val_coln="departure",
                                    plot_title=plot_title, plot_subtitle=plot_subtitle,
                                    x_label="Longitude", y_label="Latitude",
                                    size_label=size_label,
                                    #color_label="Acc\nDeparture",
                                    color_label="ARCD",
                                    hist_title="Accumulation Departure ",
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

    barPlotName <- formFileName("acc_departure_hist_", fromDate, toDate, post)
    barOutput_path <- file.path(plot_output_dir, barPlotName)
    ggsave(filename=barOutput_path, plot=map_bar_plot[[2]], units="in",
           width=6, height=4, dpi=plot_dpi)

    #-----------------------------------------------------------
    # Aggregate Accumulation Bias  --- based on new matrix
    plot_title <- "Aggregate Accumulation Bias"

    map_bar_plot <- plot_map_errors(bg_map, data_sub, xcoln="lon", ycoln="lat",
                                    size_var_coln="obs_swe_diff_sum", val_coln="acc_hit_aggbias",
                                    plot_title=plot_title, plot_subtitle=plot_subtitle,
                                    x_label="Longitude", y_label="Latitude",
                                    size_label=size_label,
                                    color_label="ARCB",
                                    hist_title="Accumulation Bias",
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

    mapPlotName <- formFileName("acc_hit_bias_map_", fromDate, toDate, post)
    barPlotName <- formFileName("acc_hit_bias_hist_", fromDate, toDate, post)
    mapOutput_path <- file.path(plot_output_dir, mapPlotName)
    barOutput_path <- file.path(plot_output_dir, barPlotName)

    ggsave(filename=mapOutput_path, plot=map_bar_plot[[1]], units="in",
           width=map_width, height=map_height, dpi=plot_dpi)

    ggsave(filename=barOutput_path, plot=map_bar_plot[[2]], units="in",
           width=6, height=4, dpi=plot_dpi)

    #
    #-----------------------------------------------------------
    # Aggregate Accumulation Bias  --- based on new matrix
    plot_title <- "Aggregate Accumulation Error"

    map_bar_plot <- plot_map_errors(bg_map, data_sub, xcoln="lon", ycoln="lat",
                                    #size_var_coln="obs_swe_diff_sum", val_coln="acc_miss_aggerror",
                                    size_var_coln="obs_swe_diff_sum", val_coln="acc_total_err",
                                    plot_title=plot_title, plot_subtitle=plot_subtitle,
                                    x_label="Longitude", y_label="Latitude",
                                    size_label=size_label,
                                    color_label="ARCE",
                                    hist_title="Accumulation Error",
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

    mapPlotName <- formFileName("acc_miss_err_map_", fromDate, toDate, post)
    barPlotName <- formFileName("acc_miss_err_hist_", fromDate, toDate, post)
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
    size_label="Total\nObserved\nAblation\n(mm)"
    #data_sub <- abl_stats
    data_sub <- all_stats[[2]]
    if (min_sample_size_n > 0) data_sub <- data_sub %>% subset(num_events >= min_sample_size_n)
    if (min_obs_ndays > 0) data_sub <- data_sub %>% subset(obs_ndays >= min_obs_ndays)
    data_sub <- data_sub %>% arrange(desc(obs_swe_diff_sum))
    plot_title <- "Aggregate Ablation Departure"
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
    barPlotName <- formFileName("abl_departure_hist_", fromDate, toDate, post)
    mapOutput_path <- file.path(plot_output_dir, mapPlotName)
    barOutput_path <- file.path(plot_output_dir, barPlotName)
    ggsave(filename=mapOutput_path, plot=map_bar_plot[[1]], units="in",
           width=map_width, height=map_height, dpi=plot_dpi)
    ggsave(filename=barOutput_path, plot=map_bar_plot[[2]], units="in",
           width=6, height=4, dpi=plot_dpi)

    #-----------------------------------------------------------
    # Aggregate Ablation Bias  --- based on new matrix
    plot_title <- "Aggregate Ablation Bias"

    map_bar_plot <- plot_map_errors(bg_map, data_sub, xcoln="lon", ycoln="lat",
                                    size_var_coln="obs_swe_diff_sum", val_coln="abl_hit_aggbias",
                                    plot_title=plot_title, plot_subtitle=plot_subtitle,
                                    x_label="Longitude", y_label="Latitude",
                                    size_label=size_label,
                                    color_label="ARBB",
                                    hist_title="Ablation Bias",
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

    mapPlotName <- formFileName("abl_hit_bias_map_", fromDate, toDate, post)
    barPlotName <- formFileName("abl_hit_bias_hist_", fromDate, toDate, post)
    mapOutput_path <- file.path(plot_output_dir, mapPlotName)
    barOutput_path <- file.path(plot_output_dir, barPlotName)
    ggsave(filename=mapOutput_path, plot=map_bar_plot[[1]], units="in",
           width=map_width, height=map_height, dpi=plot_dpi)
    ggsave(filename=barOutput_path, plot=map_bar_plot[[2]], units="in",
           width=6, height=4, dpi=plot_dpi)

    #-----------------------------------------------------------
    # Aggregate Ablation Error
    plot_title <- "Aggregate Ablation Error"

    map_bar_plot <- plot_map_errors(bg_map, data_sub, xcoln="lon", ycoln="lat",
                                    #size_var_coln="obs_swe_diff_sum", val_coln="abl_miss_aggerror",
                                    size_var_coln="obs_swe_diff_sum", val_coln="abl_total_err",
                                    plot_title=plot_title, plot_subtitle=plot_subtitle,
                                    x_label="Longitude", y_label="Latitude",
                                    size_label=size_label,
                                    color_label="ARBE",
                                    hist_title="Ablation Error",
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

    mapPlotName <- formFileName("abl_miss_err_map_", fromDate, toDate, post)
    barPlotName <- formFileName("abl_miss_err_hist_", fromDate, toDate, post)
    mapOutput_path <- file.path(plot_output_dir, mapPlotName)
    barOutput_path <- file.path(plot_output_dir, barPlotName)
    ggsave(filename=mapOutput_path, plot=map_bar_plot[[1]], units="in",
           width=map_width, height=map_height, dpi=plot_dpi)
    ggsave(filename=barOutput_path, plot=map_bar_plot[[2]], units="in",
           width=6, height=4, dpi=plot_dpi)
    #-----------------------------------------------------------
    # Aggregate Abs. Ablation Error
    plot_title <- "Aggregate Absolute Ablation Error"
    data_sub <- mutate(data_sub, abl_abs_err = abl_miss_aggerror + abs(abl_fp_err))
    map_bar_plot <- plot_map_errors(bg_map, data_sub, xcoln="lon", ycoln="lat",
                                    #size_var_coln="obs_swe_diff_sum", val_coln="abl_miss_aggerror",
                                    size_var_coln="obs_swe_diff_sum", val_coln="abl_abs_err",
                                    plot_title=plot_title, plot_subtitle=plot_subtitle,
                                    x_label="Longitude", y_label="Latitude",
                                    size_label=size_label,
                                    color_label="ARBE",
                                    hist_title="Abs. Ablation Error",
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

    mapPlotName <- formFileName("abl_miss_abs_err_map_", fromDate, toDate, post)
    barPlotName <- formFileName("abl_miss_abs_err_hist_", fromDate, toDate, post)
    mapOutput_path <- file.path(plot_output_dir, mapPlotName)
    barOutput_path <- file.path(plot_output_dir, barPlotName)
    ggsave(filename=mapOutput_path, plot=map_bar_plot[[1]], units="in",
           width=map_width, height=map_height, dpi=plot_dpi)
    ggsave(filename=barOutput_path, plot=map_bar_plot[[2]], units="in",
           width=6, height=4, dpi=plot_dpi)


    #=========================================================
    # ABLAION WITH PERSISTENT CONDITION RELATED PLOTS
    #  (need to meet ablation AND persistent conditions)
    #=========================================================

    #-----------------------------------------------------------
    # Aggregate Ablation Departure  --- under persistent condition
    # NOTE: obs_ndays for this condition need to be copied from all_stats[[2]]
    #       because obs_ndays was calculated after the persistent conditions.
    #
    # 1) rename obs_ndays in all_stats[[3]] as obs_ndays_p (number of observed days that is snow-persistent)
    all_stats[[3]] <-  all_stats[[3]] %>% rename(obs_ndays_p = obs_ndays)

    # 2) Add/Copy obs_ndays from all_stats[[2]] to all_stats[[3]]
    all_stats[[3]] <- merge(all_stats[[3]], all_stats[[2]][,c("obj_identifier", "obs_ndays")],
                            by="obj_identifier", all = F)

    plot_title <- "Aggregate Ablation Departure (Persistent Snow)"
    data_sub <- all_stats[[3]]
    if (min_sample_size_n > 0) data_sub <- data_sub %>% subset(num_events >= min_sample_size_n)
    if (min_obs_ndays > 0) data_sub <- data_sub %>% subset(obs_ndays >= min_obs_ndays) #abl condition
    #same condition as abl --75%  (number of days having obs data should be 75% of total period/days)

    #if (min_obs_ndays > 0) data_sub <- data_sub %>% subset(obs_ndays_p >= min_obs_ndays)  #current condition

    #if (min_obs_ndays > 0) data_sub <- data_sub %>% subset(obs_ndays_p >= 54)  #scaled condition
    # For own testing purpose Mean(obs_ndays_p)/Mean(obs_ndays)*75% =54%

    if (min_obs_ndays > 0) data_sub <- data_sub %>% subset(obs_ndays_p >= obs_ndays*0.5)  #new/second condition
    # At least 50% of observed data is snow-persistent

    data_sub <- data_sub %>% arrange(desc(obs_swe_diff_sum))
    map_bar_plot <- plot_map_errors(bg_map, data_sub, xcoln="lon", ycoln="lat",
                                    size_var_coln="obs_swe_diff_sum", val_coln="departure",
                                    plot_title=plot_title, plot_subtitle=plot_subtitle,
                                    x_label="Longitude", y_label="Latitude",
                                    size_label=size_label,
                                    color_label="ARBD",
                                    hist_title="Ablation Departure (Persistent Snow)",
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
    barPlotName <- formFileName("abl_departure_hist_pers_", fromDate, toDate, post)

    mapOutput_path <- file.path(plot_output_dir, mapPlotName)
    barOutput_path <- file.path(plot_output_dir, barPlotName)
    ggsave(filename=mapOutput_path, plot=map_bar_plot[[1]], units="in",
           width=map_width, height=map_height, dpi=plot_dpi)
    ggsave(filename=barOutput_path, plot=map_bar_plot[[2]], units="in",
           width=6, height=4, dpi=plot_dpi)

    #-----------------------------------------------------------
    # Aggregate Ablation with persistent condition Bias  --- based on new matrix
    plot_title <- "Aggregate Ablation Bias (Persistent Snow)"
    map_bar_plot <- plot_map_errors(bg_map, data_sub, xcoln="lon", ycoln="lat",
                                    size_var_coln="obs_swe_diff_sum", val_coln="abl_hit_aggbias",
                                    plot_title=plot_title, plot_subtitle=plot_subtitle,
                                    x_label="Longitude", y_label="Latitude",
                                    size_label=size_label,
                                    color_label="ARBB",
                                    hist_title="Ablation Bias (Persistent Snow)",
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

    mapPlotName <- formFileName("abl_hit_bias_map_pers_", fromDate, toDate, post)
    barPlotName <- formFileName("abl_hit_bias_hist_pers_", fromDate, toDate, post)

    mapOutput_path <- file.path(plot_output_dir, mapPlotName)
    barOutput_path <- file.path(plot_output_dir, barPlotName)
    ggsave(filename=mapOutput_path, plot=map_bar_plot[[1]], units="in",
           width=map_width, height=map_height, dpi=plot_dpi)
    ggsave(filename=barOutput_path, plot=map_bar_plot[[2]], units="in",
           width=6, height=4, dpi=plot_dpi)

    #-----------------------------------------------------------
    # Aggregate Ablation Error with Persistent Condition  --- based on new matrix
    plot_title <- "Aggregate Ablation Error (Persistent Snow)"

    map_bar_plot <- plot_map_errors(bg_map, data_sub, xcoln="lon", ycoln="lat",
                                    #size_var_coln="obs_swe_diff_sum", val_coln="abl_miss_aggerror",
                                    size_var_coln="obs_swe_diff_sum", val_coln="abl_total_err",
                                    plot_title=plot_title, plot_subtitle=plot_subtitle,
                                    x_label="Longitude", y_label="Latitude",
                                    size_label=size_label,
                                    color_label="ARBE",
                                    hist_title="Ablation Error (Persistent Snow)",
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

    mapPlotName <- formFileName("abl_miss_err_map_pers_", fromDate, toDate, post)
    barPlotName <- formFileName("abl_miss_err_hist_pers_", fromDate, toDate, post)

    mapOutput_path <- file.path(plot_output_dir, mapPlotName)
    barOutput_path <- file.path(plot_output_dir, barPlotName)
    ggsave(filename=mapOutput_path, plot=map_bar_plot[[1]], units="in",
           width=map_width, height=map_height, dpi=plot_dpi)
    ggsave(filename=barOutput_path, plot=map_bar_plot[[2]], units="in",
           width=6, height=4, dpi=plot_dpi)
    #-----------------------------------------------------------


    #-----------------------------------------------------------
    # Aggregate Abs. Ablation Error with Persistent Condition
    plot_title <- "Aggregate Absolute Ablation Error (Persistent Snow)"
    data_sub <- mutate(data_sub, abl_abs_err = abl_miss_aggerror + abs(abl_fp_err))

    map_bar_plot <- plot_map_errors(bg_map, data_sub, xcoln="lon", ycoln="lat",
                                    size_var_coln="obs_swe_diff_sum", val_coln="abl_abs_err",
                                    plot_title=plot_title, plot_subtitle=plot_subtitle,
                                    x_label="Longitude", y_label="Latitude",
                                    size_label=size_label,
                                    color_label="ARBE",
                                    hist_title="Abs. Ablation Error (Persistent Snow)",
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

    mapPlotName <- formFileName("abl_miss_abs_err_map_pers_", fromDate, toDate, post)
    barPlotName <- formFileName("abl_miss_abs_err_hist_pers_", fromDate, toDate, post)

    mapOutput_path <- file.path(plot_output_dir, mapPlotName)
    barOutput_path <- file.path(plot_output_dir, barPlotName)
    ggsave(filename=mapOutput_path, plot=map_bar_plot[[1]], units="in",
           width=map_width, height=map_height, dpi=plot_dpi)
    ggsave(filename=barOutput_path, plot=map_bar_plot[[2]], units="in",
           width=6, height=4, dpi=plot_dpi)
    #-----------------------------------------------------------
    #message("Done\n")

}

