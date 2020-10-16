# Functions to process swe data
################################ Functions Below 
connect_database <- function(base_db_path) {
  library(RSQLite)
  
  base <- DBI::dbConnect(RSQLite::SQLite(), base_db_path) 
  # call dbDisconnect(base) when it's done
  nwm_base_db_name <- basename(base_db_path)
  nwm_land_db_name <- gsub("base", "land_single", nwm_base_db_name)
  nwm_land_db_path <- file.path(db_dir, nwm_land_db_name)
  land <- DBI::dbConnect(RSQLite::SQLite(), nwm_land_db_path)
  # call dbDisconnect(land) when it's done
  
  #Get database info
  src_dbi(base)
  as.data.frame(dbListTables(base))  # or this
  src_dbi(land)
  as.data.frame(dbListTables(land))
  #To list fields of a table:
  as.data.frame(dbListFields(base, "databases_info"))
  # or
  dbListFields(base, "databases_info") #where databases_info is one of table names in base
  
  dbDisconnect(base) 
  dbDisconnect(land) 
}
#----------------------------------------------------
#---------------------------------------------------- 
get_nwm_wdb_stat_fnames <- function(data_path,
                                    fromDate,
                                    toDate,
                                    target_hour,
                                    hr_range) {

  hours_info <- two_hour_ends(target_hour, hr_range)
  minus_hour <- hours_info[1]
  plus_hour <- hours_info[2]
  
  nwm_pre <- "nwm_"

  if (target_hour == minus_hour & target_hour == plus_hour) {
    post <- paste0(".csv")
  } else {
    post <- paste0("_at", target_hour, "z_",
                   "from", minus_hour, "to", plus_hour, ".csv")
  } 
  nwm_processed_name <- formFileName(nwm_pre, fromDate, toDate, post)
  nwm_processed_path <- file.path(data_path, nwm_processed_name)
  
  wdb_pre <- "wdb_"
  wdb_processed_name <- formFileName(wdb_pre, fromDate, toDate, post)
  wdb_processed_path <- file.path(data_path, wdb_processed_name)
  
  
  fpaths_list <- list(nwm_processed_path,
                      wdb_processed_path)
                    
  return (fpaths_list)
}
#----------------------------------------------------
#---------------------------------------------------- 

get_data_via_py <- function(nwm_base_db_path,
                            fromDate,
                            toDate,
                            no_data_value=-99999.0,
                            bounding_box=NA,
                            target_hour=NA,
                            hr_range=NA,
                            verbose=NA) {
    
  library(gsubfn)  # In order to use list[a, b] <- functionReturningTwoValues()
  #library("reticulate", lib.loc="~/R/x86_64-redhat-linux-gnu-library/3.6") #for dw7
  library("reticulate")
  use_python("/usr/bin/python3.6")  #define which version of python to use - dw7
  #use_python("/usr/bin/python3.8")  #define which version of python to use -dw9
  source_python("query_nwm_wdb_data.py") #load the python modules
  message("bounding box values:")
  print(bounding_box)
  list[nwm_data_py, wdb_data_py] <- py_get_data_main(nwm_base_db_path,
                                                     fromDate,
                                                     toDate,
                                                     no_data_value,
                                                     bounding_box,
                                                     target_hour,
                                                     hr_range,
                                                     verbose)
 
  message("Successully returned data from Python function: py_get_data_main")

  # #Convert datetime back to original (UTC time) as below
  nwm_data_py$datetime <- py_to_r(r_to_py(as.POSIXct(
     strptime(nwm_data_py$datetime,format="%Y-%m-%d %H:%M:%S"))))
   wdb_data_py$datetime <- py_to_r(r_to_py(as.POSIXct(
     strptime(wdb_data_py$datetime,format="%Y-%m-%d %H:%M:%S"))))
  # 
  message("Returning NWM and WDB data to main script")
  return (list(nwm_data_py, wdb_data_py))
}
#----------------------------------------------------
#---------------------------------------------------- 
date_string_to_date_ymdh <- function(date_string) {
    # Convert date string in yyyy-mm-dd hh:00:00 to yyyymmddhh
    
    data_ymdh <- paste0(date_string %>% substring(1,4),
                        date_string %>% substring(6,7),
                        date_string %>% substring(9,10),
                        date_string %>% substring(12,13))
    return (data_ymdh)

}
#----------------------------------------------------
#---------------------------------------------------- 
get_time_related_fname <- function(pre,
                                   domain_text,
                                   fromDate,
                                   toDate,
                                   target_hour=NA,
                                   hr_range=NA) {
    pre <- paste0(pre, 
                  gsub("-", "", fromDate %>% substring(1,10)),
                  fromDate %>% substring(12,13), "_", 
                  gsub("-", "",toDate %>% substring(1,10)),
                  toDate %>% substring(12,13))
    
    if (target_hour < 0 || is.na(target_hour)) {
        post <- paste0(".csv")
    } else {
        post <- paste0("_", target_hour, 
                       "zm", hr_range[1],
                       "p", hr_range[2],
                       "_", domain_text, ".csv")
    }
    
    fname <- paste0(pre, post)
    #bias_file_name <- formFileName(pre, fromDate, toDate, post)
    return (fname)
}
#----------------------------------------------------
#---------------------------------------------------- 

date_period <- function(date_num) {
    year <- date_num %>% substring(1 ,4)
    month <- date_num %>% substring(5, 6)
    day <- date_num %>% substring(7, 8)
    hour <- date_num %>% substring(9, 10)
    fromDate <- paste0(year, "-", month, "-", day, " ", hour, ":00:00")
  
}
#---------------------------------
#---------------------------------
two_hour_ends <- function(target_hour, hr_range) {
    minus_hour <- target_hour - hr_range[1]
    plus_hour <- target_hour + hr_range[2]
    
    if (minus_hour < 0) { minus_hour <- 24 - minus_hour }  # assume 0-23 hours
    if (plus_hour > 23) { plus_hour <- plus_hour - 24 }
    
    return (list(minus_hour, plus_hour))
  
}
#----------------------------------------------------
#---------------------------------------------------- 
formFileName <- function(pre, fromDate, toDate, post) {
    fname <- paste0(pre,
                    gsub("-", "",fromDate %>% substring(1,10)),
                    #fromDate %>% substring(12,13),
                    "_",
                    gsub("-", "",toDate %>% substring(1,10)),
                    #toDate %>% substring(12,13),
                    post)
  
}
#------------------------------------
#------------------------------------
pick_near_h_datetime <- function(df_each_day, target_hour, h_range){
    #Find the nearest datetime in this day to be picked
    
    #h_min <- min(abs(hour(df_each_day$datetime)-target_hour))
    h_min <- min(hour(df_each_day$datetime)-target_hour)
    
    cell_loc <- which.min(abs(hour(df_each_day$datetime)-target_hour))
    datetime_cell <- df_each_day$datetime[cell_loc]
    
    if (h_min < 0 & h_min > (h_range[1] - target_hour)) { return (datetime_cell)}
    else if (h_min > 0 & h_min < (h_range[2] - target_hour)) { return (datetime_cell)}
    else {return ("")}

    # if (h_min < h_range) {
    #     return (datetime_cell)
    # } else {
    #     return ("")
    # }
  
}
#--------------------------------------------------------------------------
#--------------------------------------------------------------------------
subset_short_for_period <- function(db_fromDate, db_toDate,
                                    fromDate, toDate,
                                    nwm_csv_ori_path, wdb_csv_ori_path,
                                    target_hour=NA,
                                    hr_range=c(NA, NA)) {
    
    from_hour = target_hour - hr_range[1]
    to_hour = target_hour + hr_range[2]
    if (from_hour < 0) {from_hour = 24 - from_hour}
    if (to_hour > 23)  {to_hour = to_hour - 24}
    
    
    #Extract data from the queried for a shorther period
    nwm_swe_ori <- read.csv(nwm_csv_ori_path)
    wdb_swe_ori <- read.csv(wdb_csv_ori_path)
    
    nwm_swe_ori$datetime <- strptime(as.character(nwm_swe_ori$datetime),
                                                  format="%Y-%m-%d %H:%M:%S")
    nwm_swe_ori$datetime <- as.character(nwm_swe_ori$datetime)
    nwm_swe_sub <- nwm_swe_ori %>% filter(datetime >= as.POSIXct(fromDate) &
                                              datetime <= as.POSIXct(toDate) &
                                              hour(datetime)>=from_hour &
                                              hour(datetime)<=to_hour)
    
    wdb_swe_ori$datetime <- strptime(as.character(wdb_swe_ori$datetime),
                                                  format="%Y-%m-%d %H:%M:%S")
    wdb_swe_ori$datetime <- as.character(wdb_swe_ori$datetime)
    wdb_swe_sub <- wdb_swe_ori %>% filter(datetime >= as.POSIXct(fromDate) &
                                              datetime <= as.POSIXct(toDate) & 
                                              hour(datetime)>=from_hour &
                                              hour(datetime)<=to_hour)
    return (list(nwm_swe_sub, wdb_swe_sub))
}

#--------------------------------------------------------------------------
#--------------------------------------------------------------------------


sample_nwm_wdb_data <- function(nwm_in, wdb_in,
                                target_hour=NA, hr_range=c(NA, NA)) {
    #sample data for common datetime between nwm_in and wdb_in depending on
    #values of target_hour, hr_range. nwm_in and wdb_in are having common stations now
    #before this function is called.
    #Default is including all hours of data
    
    `%notin%` <- Negate(`%in%`)
    
    #nwm_in <- nwm_in_wdb  #for local debug purpose
    #wdb_in <- wdb_in_nwm
    
    #check number of unique stations
    num_stations_nwm <- n_distinct(nwm_in$obj_identifier)
    num_stations_wdb <- n_distinct(wdb_in$obj_identifier)
    if (num_stations_nwm == num_stations_wdb) {
        print(paste0("Same number of stations for nwm and wdb --> ", 
                     num_stations_wdb))
    } else {
        stop("The number of stations in two datasets does not match!")
        #warning("The number of stations in two datasets does not match!")
    }
    
    #Initializing two dataframes to hold final data of all stations
    nwm_com <- data.frame(obj_identifier = integer(),
                          datetime = character(),
                          swe = numeric())
    wdb_com <- data.frame(obj_identifier = integer(),
                          station_id = character(),
                          lon = numeric(),
                          lat = numeric(),
                          elevation = numeric(),
                          datetime = character(),
                          obs_swe_mm = numeric())
    
    station_count <- 0
    for(g in unique(nwm_in$obj_identifier)) {  #for each station
        station_count <- station_count + 1
        message("Processing data for station ", g, " : ", station_count, 
                " of ", num_stations_wdb)
        #stop("the first station is ", g)
        nwm_group <- subset(nwm_in, subset = obj_identifier == g)
        wdb_group <- subset(wdb_in, subset = obj_identifier == g)
        
        if (target_hour < 0 || is.na(target_hour)) {  #case 1
            #for the same station/group, find all the data for common datetimes
            nwm_com <- rbind(nwm_com, nwm_group[nwm_group$datetime %in%
                                                    wdb_group$datetime,])
            wdb_com <- rbind(wdb_com, wdb_group[wdb_group$datetime %in%
                                                    nwm_group$datetime,])
        # } else if (target_hour >= 0 && hr_range <= 0) {  #case 2
        #     #find common data at the target_hour for all every day that exisit in wdb
        #     #One value each day if wdb@target_hour exist
        #     nwm_com <- rbind(nwm_com, nwm_group %>%
        #                          filter(hour(datetime) == target_hour)) 
        #     wdb_com <- rbind(wdb_com, wdb_group %>%
        #                          filter(hour(datetime) == target_hour)) 
        } else { # case 3: target_hour >= 0 && hr_range > 0

            #find common data at the target_hour plus at the nearest hour 
            #  if target_hour wdb data is missing
            
            #first find available target_hour data and will be temprorily 
            #  excluded in finding near hour
            wdb_g_at_hour <- wdb_group %>%
                filter(hour(datetime) == target_hour)
            
            #Add new column as ymd - a string of yyyymmdd, to be used in notin later
            wdb_group <- wdb_group %>%
                mutate(ymd=paste(format(as.Date(datetime,format="%Y-%m-%d"),
                                        format = "%Y%m%d")))
            wdb_g_at_hour <- wdb_g_at_hour %>% 
                mutate(ymd=paste(format(as.Date(datetime,format="%Y-%m-%d"),
                                        format = "%Y%m%d")))
            
            
            # create a tmp df to hold those that without target_hour data in these yyyymmdd
            wdb_g_near_h_tmp <- wdb_group[wdb_group$ymd %notin%
                                              wdb_g_at_hour$ymd, ]
            
            wdb_g_near_h <- slice(wdb_com, 0) #initializing
            #wdb_g_near_h <- wdb_com #initializing
            
            #loop through all days and find the data at the nearest hour for each day
            for(ymdv in unique(wdb_g_near_h_tmp$ymd)) {
                wdb_g_near_h_each_day <- wdb_g_near_h_tmp %>% filter(ymd == ymdv)
                near_h_datetime <- pick_near_h_datetime(wdb_g_near_h_each_day,
                                                        target_hour, hr_range)
                if (nchar(near_h_datetime) == 19) {  #datetime length
                    wdb_g_near_h_d <- wdb_g_near_h_each_day %>%
                        filter(datetime == near_h_datetime) %>% 
                        select(-ymd)
                    #wdb_g_at_and_near_h <- rbind(wdb_g_at_and_near_h, wdb_g_near_h_d)
                    
                    wdb_g_near_h <- rbind(wdb_g_near_h, wdb_g_near_h_d)
                }
            }
            
            wdb_com_g <- rbind(wdb_g_at_hour %>% select(-ymd), wdb_g_near_h)
            wdb_com_g <- wdb_com_g %>% arrange(datetime)
            wdb_com_g <- wdb_com_g[wdb_com_g$datetime %in% nwm_group$datetime, ] #make
            # sure that values in wdb are also in nwm
            wdb_com <- rbind(wdb_com, wdb_com_g)
            
            nwm_com_g <- nwm_group[nwm_group$datetime %in% wdb_com_g$datetime,]
            nwm_com <- rbind(nwm_com, nwm_com_g)
            
            #Combine nwm_com and wdb_com into one datafraem
            #nwm_com <- nwm_com %>% rename(nwm_swe = swe) # change name from swe to nwm_swe
            nwm_wdb_com_daily <- merge(wdb_com, nwm_com, by=c("obj_identifier", "datetime"), all = T)
            
            
        }  #end of case 2 - else
        #stop("First station has finished for ", g) 
    } # end of station/g loop
    nwm_wdb_com_daily <- nwm_wdb_com_daily %>% rename(nwm_swe = swe) # change name from swe to nwm_swe
    rm(nwm_com, wdb_com)
    return (nwm_wdb_com_daily)
  
}
#--------------------------------------------------------------------------
#--------------------------------------------------------------------------
calculate_bias_stations <- function(nwm_in_wdb, wdb_in_nwm, target_hour=-1) {
  #Calculate bias between two sets of data
  
  #check number of unique stations
  num_stations_nwm <- n_distinct(nwm_in_wdb$obj_identifier)
  num_stations_wdb <- n_distinct(wdb_in_nwm$obj_identifier)
  if (num_stations_nwm == num_stations_wdb) {
    print(paste0("Same number of stations for nwm and wdb --> ", 
                 num_stations_wdb))
  } else {
    stop("The number of stations in two datasets does not match!")
    #warning("The number of stations in two datasets does not match!")
  }
  
  
  #Loop through each station and calculate statistics
  #col_names <- c("obj_identifier", "lat","lon","pbias","mean_swe","n_samples")
  result_df <- data.frame(obj_identifier = integer(),
                          station_id = character(),
                          lat = numeric(),
                          lon = numeric(),
                          elevation = numeric(),
                          pbias = numeric(),
                          mean = numeric(),
                          n_samples = integer())  #Defining the dataframe 
  #result_df <- setNames(data.frame(matrix(ncol = 6, nrow = 0)), col_names)
  #num_unique_station <- length(unique(nwm_in_wdb$obj_identifier))
  
  station_count <- 0
  for(g in unique(nwm_in_wdb$obj_identifier)) {
    station_count <- station_count + 1
    message("Calculating bias for ", g, " : ", 
            station_count, " of ", num_stations_wdb)
    
    nwm_group <- subset(nwm_in_wdb, subset = obj_identifier == g)
    wdb_group <- subset(wdb_in_nwm, subset = obj_identifier == g)
    wdb_group <- wdb_group %>% select(obj_identifier, station_id,
                                        lon, lat, elevation, datetime, obs_swe_mm)
    
    wdb_group$nwm_swe <- nwm_group$swe
    wdb_group$swe_diff <- wdb_group$nwm_swe - wdb_group$obs_swe_mm
    
    if (sum(wdb_group$obs_swe_mm) != 0.0) {
      pbias <- 100*sum(wdb_group$swe_diff)/sum(wdb_group$obs_swe_mm)
      obj <- wdb_group$obj_identifier[1]
      sta_id <- wdb_group$station_id[1]
      elev <- wdb_group$elevation[1]
      lat <- wdb_group$lat[1]
      lon <- wdb_group$lon[1]
      
      mean_swe <- mean(wdb_group$obs_swe_mm, na.rm = T)
      n_samples <- nrow(wdb_group)
      #cat("obj=", obj, " pbias=", pbias, "\n")
      
      result_df <- rbind(result_df, 
                         data.frame(obj_identifier=obj,
                                    station_id=sta_id,
                                    lat=lat,
                                    lon=lon,
                                    elevation=elev,
                                    pbias=pbias,
                                    mean_swe=mean_swe,
                                    n_samples=n_samples))
    }
    
  } # end of g loop
  
  return(result_df)
}
#--------------------------------------------------------------------------
#--------------------------------------------------------------------------
acc_abl_scores <- function(swe_acc_abl_com,
                           acc_abl_option) {

  # Calculate different scores based on 2020 Q4 document.
  # The acc_abl_option indicated whether it's for accumulation (>0)
  # or for ablation (<0)
  # This function moves some content from acc_abl_analysis() to here
  # 
  # Originally there are three dataframes for departure, hit and miss
  # Now I combined them into one and added station_is and name to the 
  # combined table (for acc, abl, and abl_pers)
  
  # if (acc_abl_option == 0) {
  #   stop("acc_abl_option should be either >0 (acc) or <0 (alb)")
  # }
  
  
  #Based on new ideas (see 2020Q4 doc)
    
  #Get the total number of events that have observed SWE
  num_events <- swe_acc_abl_com %>% group_by(obj_identifier) %>%
      filter(!is.na(wdb_swe_diff)) %>% 
      summarise(total_num_events=n())
  
  num_events_having_diff <- swe_acc_abl_com %>% group_by(obj_identifier) %>%
      filter(abs(wdb_swe_diff) > 0) %>%
      summarise(num_events_diff=n())
  
  obs_ndays <- swe_acc_abl_com %>% group_by(obj_identifier) %>%
      filter(!is.na(obs_swe_mm)) %>% 
      summarise(obs_ndays=n())  
  
  # Basic stats for both accumulation and ablation
  stats <- swe_acc_abl_com %>% group_by(obj_identifier) %>% 
    filter(if (acc_abl_option > 0) wdb_swe_diff > 0 else wdb_swe_diff <0 ) %>%
           #else if (acc_abl_option < 0) wdb_swe_diff <0 ) %>%
    summarise(station_id = first(station_id),
              station_name = first(name),
              lon = first(lon),
              lat = first(lat),
              elevation = first(elevation),
              mean_obs_swe = mean(obs_swe_mm),
              mean_nwm_swe = mean(nwm_swe),
              obs_swe_sum = sum(obs_swe_mm),
              nwm_swe_sum = sum(nwm_swe),
              obs_swe_diff_sum = ifelse((acc_abl_option > 0),
                                        sum(wdb_swe_diff),
                                        -sum(wdb_swe_diff)),
              nwm_swe_diff_sum = ifelse((acc_abl_option > 0),
                                        sum(nwm_swe_diff),
                                        -sum(nwm_swe_diff)),
              departure = ifelse((acc_abl_option > 0), 
                                 sum(nwm_swe_diff - wdb_swe_diff)/sum(wdb_swe_diff),
                                 -sum(nwm_swe_diff - wdb_swe_diff)/sum(wdb_swe_diff)),
              num_wdb_diff=n())
  
  #stats <- cbind(stats, swe_sum[, "obs_swe_diff_sum"])
  if (acc_abl_option != 0) {
      stats <- merge(stats, num_events, by="obj_identifier", all = T) #Add this column
      stats <- merge(stats, obs_ndays, by="obj_identifier", all = T) #Add this column 
  }
  
  
  #for hit case:
  if (acc_abl_option > 0) {
      acc_hit <- swe_acc_abl_com %>% group_by(obj_identifier) %>%
          filter(wdb_swe_diff > 0 & nwm_swe_diff > 0) %>% 
          summarise(acc_hit_diff_sum = sum(nwm_swe_diff - wdb_swe_diff),
                    acc_hit_obs_swe_diff_sum = sum(wdb_swe_diff),
                    acc_hit_num_wdb_diff=n())
      #acc_hit <-  mutate(acc_hit, acc_hit_aggbias = acc_hit_diff_sum/acc_hit_obs_swe_diff_sum)
      stats <- stats %>% subset(!is.na(obs_swe_diff_sum))  # Get rid of NA cases
      stats <- merge(stats, acc_hit, by="obj_identifier", all = T) #Add this column
      stats <- mutate(stats, acc_hit_aggbias = acc_hit_diff_sum/obs_swe_diff_sum)
      #acc_hit <-  mutate(acc_hit, acc_hit_aggbias = acc_hit$acc_hit_diff_sum/stats$obs_swe_diff_sum)
      
  } else if (acc_abl_option < 0) {
      abl_hit <- swe_acc_abl_com %>% group_by(obj_identifier) %>%
          filter(wdb_swe_diff < 0 & nwm_swe_diff < 0) %>% 
          summarise(abl_hit_diff_sum = sum(nwm_swe_diff - wdb_swe_diff),
                    abl_hit_obs_swe_diff_sum = sum(wdb_swe_diff),
                    abl_hit_num_wdb_diff=n())
      #abl_hit <-  mutate(abl_hit, abl_hit_aggbias = abl_hit_diff_sum/abl_hit_obs_swe_diff_sum)
      stats <- stats %>% subset(!is.na(stats$obs_swe_diff_sum))  # Get rid of NA cases
      stats <- merge(stats, abl_hit, by="obj_identifier", all = T) #Add its columns
      stats <- mutate(stats, abl_hit_aggbias = abl_hit_diff_sum/obs_swe_diff_sum)
      #abl_hit <-  mutate(abl_hit, abl_hit_aggbias = abl_hit$abl_hit_diff_sum/stats$obs_swe_diff_sum)
  }
  # } else {
  #     #stop("Incorrect acc_abl_option!")
  #     message("No obs_swe_diff case!")
  # }
   

  if (acc_abl_option > 0) {
      #acc_hit$acc_hit_aggbias[is.na(acc_hit$acc_hit_aggbias)] <- 0.0001
      stats$acc_hit_aggbias[is.na(stats$acc_hit_aggbias)] <- 0.0001
  } else if (acc_abl_option < 0){
      #abl_hit$abl_hit_aggbias[is.na(abl_hit$abl_hit_aggbias)] <- 0.0001
      stats$abl_hit_aggbias[is.na(stats$abl_hit_aggbias)] <- 0.0001
  }

  
  #For miss case:
  if (acc_abl_option > 0) {
      acc_miss <- swe_acc_abl_com %>% group_by(obj_identifier) %>%
          filter(wdb_swe_diff > 0 & nwm_swe_diff <= 0) %>% 
          summarise(acc_miss_diff_sum = sum(nwm_swe_diff - wdb_swe_diff),
                    acc_miss_obs_swe_diff_sum = sum(wdb_swe_diff),
                    acc_miss_num_wdb_diff=n())
      #acc_miss <-  mutate(acc_miss, acc_miss_aggerror = acc_miss_diff_sum/acc_miss_obs_swe_diff_sum)
      
      stats <- merge(stats, acc_miss, by="obj_identifier", all = T) #Add its columns
      stats <- mutate(stats, acc_miss_aggerror = acc_miss_diff_sum/obs_swe_diff_sum)
      
  } else if (acc_abl_option < 0) {
      abl_miss <- swe_acc_abl_com %>% group_by(obj_identifier) %>%
          filter(wdb_swe_diff < 0 & nwm_swe_diff >= 0) %>% 
          summarise(abl_miss_diff_sum = sum(nwm_swe_diff - wdb_swe_diff),
                    abl_miss_obs_swe_diff_sum = sum(wdb_swe_diff),
                    abl_miss_num_wdb_diff=n())
      #abl_miss <-  mutate(abl_miss, abl_miss_aggerror = abl_miss_diff_sum/abl_miss_obs_swe_diff_sum)
      stats <- merge(stats, abl_miss, by="obj_identifier", all = T) #Add its columns
      stats <- mutate(stats, abl_miss_aggerror = abl_miss_diff_sum/obs_swe_diff_sum)
  }   
  # } else {
  #     stop("Incorrect acc_abl_option!")
  # }
  
  
  if (acc_abl_option > 0) {
      stats$acc_miss_aggerror[is.na(stats$acc_miss_aggerror)] <- 0.0001
  } else if (acc_abl_option < 0){
      stats$abl_miss_aggerror[is.na(stats$abl_miss_aggerror)] <- 0.0001
  }
  
  
  
  
  # To count for the cells 4, 5, and 6 in the contingent table (3x3 table)
  if (acc_abl_option == 0) {
      # simple counting for cases when no acc and abl
      stats <- swe_acc_abl_com %>% group_by(obj_identifier) %>% 
          filter(wdb_swe_diff == 0) %>%
                 summarise(station_id = first(station_id),
                           station_name = first(name),
                           lon = first(lon),
                           lat = first(lat),
                           elevation = first(elevation),
                           mean_obs_swe = mean(obs_swe_mm),
                           mean_nwm_swe = mean(nwm_swe),
                           obs_swe_sum = sum(obs_swe_mm),
                           nwm_swe_sum = sum(nwm_swe),
                           ndays_no_wdb_diff = n())
      both_no_swe_diff <- swe_acc_abl_com %>% group_by(obj_identifier) %>% 
          filter(wdb_swe_diff == 0 & nwm_swe_diff == 0.0) %>%  # no case for nwm_swe_diff == 0
          summarise(ndays_no_swe_diff = n())
      stats <- merge(stats, both_no_swe_diff, by="obj_identifier", all = T) #Add this column
      stats <- merge(stats, num_events_having_diff, by="obj_identifier", all = T) #Add this column
      stats <- merge(stats, num_events, by="obj_identifier", all = T) #Add this column
      stats <- merge(stats, obs_ndays, by="obj_identifier", all = T) #Add this column
      stats <- stats %>% subset(stats$obs_swe_sum > 0)
      #stats <- stats %>% subset(!is.na(stats$obs_swe_sum))
      
      #
<<<<<<< HEAD
      #stats <- swe_acc_abl_com %>% group_by(obj_identifier) %>% 
      #    filter(wdb_swe_diff == 0 & nwm_swe_diff >=0.0) %>%
=======
      stats <- swe_acc_abl_com %>% group_by(obj_identifier) %>% 
          filter(wdb_swe_diff == 0 & nwm_swe_diff >=0.0) %>%
>>>>>>> nwm_da_zzhang/develop
  }
  
  

  return (stats)

}
#--------------------------------------------------------------------------          
#--------------------------------------------------------------------------
acc_abl_analysis <- function(nwm_wdb_com_daily) {

  library(lubridate)
    
  # Exclude stations that don't have enough data (<min_obs_ndays)
  nwm_wdb_com_daily$datetime <- ymd_hms(nwm_wdb_com_daily$datetime)
  swe_acc_abl_com <- nwm_wdb_com_daily %>% group_by(obj_identifier) %>%
      mutate(wdb_swe_diff = case_when(month(datetime)==month(lag(datetime)) &
                                          (day(datetime)-day(lag(datetime)))==1
                                      ~ obs_swe_mm-lag(obs_swe_mm),
                                      (month(datetime)-month(lag(datetime)))==1 &
                                          day(datetime) == 1
                                      ~ obs_swe_mm-lag(obs_swe_mm),
                                      (month(datetime)-month(lag(datetime)))==-11 &
                                          day(datetime) == 1
                                      ~ obs_swe_mm-lag(obs_swe_mm),
                                      TRUE ~ NA_real_),
             nwm_swe_diff = case_when(month(datetime)==month(lag(datetime)) &
                                          (day(datetime)-day(lag(datetime)))==1
                                      ~ nwm_swe-lag(nwm_swe),
                                      (month(datetime)-month(lag(datetime)))==1 &
                                          day(datetime) == 1
                                      ~ nwm_swe-lag(nwm_swe),
                                      (month(datetime)-month(lag(datetime)))==-11 &
                                          day(datetime) == 1
                                      ~ nwm_swe-lag(nwm_swe),
                                      TRUE ~ NA_real_),
             swe_ratio=nwm_swe/obs_swe_mm,
             persist = case_when(month(datetime)==month(lag(datetime)) &
                                     (day(datetime)-day(lag(datetime)))==1 &
                                     nwm_swe >0 & lag(nwm_swe) > 0 & 
                                     obs_swe_mm > 0 & lag(obs_swe_mm) > 0
                                 ~ 1,
                                 (month(datetime)-month(lag(datetime)))==1 &
                                     day(datetime) == 1 &
                                     nwm_swe >0 & lag(nwm_swe) > 0 & 
                                     obs_swe_mm > 0 & lag(obs_swe_mm) > 0
                                 ~ 1,
                                 (month(datetime)-month(lag(datetime)))==-11 &
                                     day(datetime) == 1 &
                                     nwm_swe >0 & lag(nwm_swe) > 0 & 
                                     obs_swe_mm > 0 & lag(obs_swe_mm) > 0
                                 ~ 1,
                                 TRUE ~ 0))
  
  # Accumulation case -- acc*  
  acc_abl_option <- 1
  acc_stats <- acc_abl_scores(swe_acc_abl_com,
                              acc_abl_option)
  #Now combine used to be stats, hit, and miss into one dataframe/table
  
  # Ablation case -- abl*
  acc_abl_option <- -1
  abl_stats <- acc_abl_scores(swe_acc_abl_com,
                              acc_abl_option)
  
  # Ablation under persistent option  -- abl_pers*
  acc_abl_option <- -1
  swe_acc_abl_com_pers <-  swe_acc_abl_com %>% subset(persist == 1)
  abl_per_stats <- acc_abl_scores(swe_acc_abl_com_pers,
                                  acc_abl_option)
  
  # Add another case when acc_abl_option = 0 for obs_swe_diff = 0
  # This is to deal with the cells 4, 5, and 6 in the contingent table
  acc_abl_option <- 0
  no_diff_stats <- acc_abl_scores(swe_acc_abl_com,
                              acc_abl_option)
  
  return (list(acc_stats, abl_stats, abl_per_stats, no_diff_stats))
}

# END OF FUNCTIONS--------------------------------------
