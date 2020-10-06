# Functions related to plots
#-----------------------------------------------------------
map_setup <- function(df=NULL, min_lat=NULL, max_lat=NULL, min_lon=NULL, max_lon=NULL,
                     minLatAdj=0, maxLatAdj=0, minLonAdj=0, maxLonAdj=0) {
  
  lats<-df$lat
  lons<-df$lon
  # clat <- mean(lats)
  # clon <- mean(lons)

  min_lat <- ifelse(is.null(min_lat), min(lats), min_lat)
  max_lat <- ifelse(is.null(max_lat), max(lats), max_lat)
  min_lon <- ifelse(is.null(min_lon), min(lons), min_lon)
  max_lon <- ifelse(is.null(max_lon), max(lons), max_lon)
  myBox <- c(min_lon+minLonAdj, min_lat+minLatAdj,
                  max_lon+maxLonAdj, max_lat+maxLatAdj)
  #myLocation <- c(minLon+4, minLat+3, maxLon-3, maxLat-6)
  bg_map <- ggmap::get_map(location=myBox, source="stamen", maptype="terrain", crop=TRUE)
  #myMap <- ggmap::get_googlemap(center=c(lon=clon, lat=clat), zoom=4, maptype="terrain", format="png8", size=c(640,480), scale=2)
  
  ##Or just use two lines before for the background map
  #data_bbox <- ggmap::make_bbox(lat = lat, lon = lon, data = df, f = 0.1)  #bounding box
  #bg_map <- get_stamenmap(data_bbox, maptype = "terrain", zoom=5)
  
  return (bg_map)
}

#---------------------------------------------------------------------
plot_map_errors <- function(bg_map, stats_df, xcoln="lon", ycoln="lat",
                            size_var_coln="mean_swe", val_coln="pbias",
                            plot_title="Model Errors", plot_subtitle="",
                            x_label="Longitude", y_label="Latitude",
                            size_label="Mean Absolute Error",
                            color_label="Mean Signed Bias",
                            hist_title="Mean Signed Bias",
                            color_breaks,
                            size_min_pt=1, size_max_pt=8,
                            color_low="blue", color_mid="white", color_high="red",
                            val_size_min_lim=NULL, val_size_max_lim=NULL,
                            min_thresh_colr=NULL, max_thresh_colr=NULL,
                            #excl_var=NULL, excl_thresh=NULL,
                            #excl_var="n_samples", excl_thresh=30,
                            val_breaks, alpha_val=0.8, histlim=NULL) {

  if (!is.data.frame(stats_df)) as.data.frame(stats_df)
  # if (!is.null(excl_var)) {
  #   my_data <- subset(stats_df, stats_df[,excl_var] <= excl_thresh)
  # } else {
  #   my_data <- stats_df
  # }
  my_data <- stats_df
  my_data$plot_cat <- cut(my_data[, val_coln], breaks = val_breaks, right = TRUE)
  my_data <- subset(my_data, !is.na(my_data$plot_cat))
  fill_val_coln <- "plot_cat"  # based on categorized values
  #valBreaksScaled <- scales::rescale(val_breaks, from=range(stats_df[,val_coln],
  #                                   na.rm=TRUE,finite=TRUE))
  if (is.null(val_size_min_lim)) val_size_min_lim <- min(my_data[,size_val_coln], na.rm=TRUE)
  if (is.null(val_size_max_lim)) {
      val_size_max_lim <- max(my_data[, size_var_coln], na.rm=TRUE)
  } else if (val_size_max_lim < max(my_data[, size_var_coln], na.rm=TRUE)) {
      message('Warning: Maximum data value is greater than the limit.')
  }

  if (is.null(min_thresh_colr)) min_thresh_colr <- min(my_data[, val_coln], na.rm=TRUE)
  if (is.null(max_thresh_colr)) min_thresh_colr <- max(my_data[, val_coln], na.rm=TRUE)
  
  #check for the val_size_max_lim
  if (!is.null(val_size_min_lim)) {
      
  }
  
  # #Bias Map - test
  # gg_err <- ggmap::ggmap(bg_map) + 
  #     ggforce::geom_circle(aes(x0=lon, y0=lat, r=obs_swe_diff_sum, fill=aggerror),
  #                          data=abl_miss) + coord_fixed()
  # 
  # gg_err <- ggmap::ggmap(bg_map) + 
  #     ggforce::geom_circle(aes(x0=xcoln, y=ycoln, r=size_var_coln, fill=fill_val_coln),
  #                         data=abl_miss) + coord_fixed()

  
  
  
  #Bias Map
  gg_err <- ggmap::ggmap(bg_map) +
    ggplot2::geom_point(aes_string(x=xcoln, y=ycoln, size=size_var_coln, fill=fill_val_coln),
                        data=my_data, alpha=alpha_val, shape=21) +
    ggplot2::scale_radius(size_label, range=c(size_min_pt, size_max_pt),
                        limits=c(val_size_min_lim, val_size_max_lim)) + 
    ggplot2::scale_fill_manual(color_label, values=color_breaks, drop=FALSE) +
    #ggplot2::scale_fill_gradient2(color_label, low=color_low, mid=color_mid, 
    #                              high=color_high, midpoint=0,
    #                              limits=c(min_thresh_colr, max_thresh_colr)) +
    #ggplot2::scale_fill_gradientn(color_label, colours = color_reaks,
    #                              values = scales::rescale(val_breaks)) +
    ggplot2::ggtitle(bquote(atop(.(plot_title), atop(italic(.(plot_subtitle)), "")))) +
    ggplot2::labs(x = x_label, y = y_label) +
    ggplot2::theme(plot.title=element_text(size=16,face="bold", vjust=-1, hjust=0.5)) +
    ggplot2::guides(fill=guide_legend(override.aes=list(size=3), order=1), size=
                    guide_legend(order=2))
    #NOTE: scale_size was replaced by scale_radius (area vs radius).   
    #      Tried scale_size_binned() and scale_size_area(). scale_radius is the best
  
  gg_hist <- ggplot(data=subset(my_data, !is.na(val_coln)),
                    aes(plot_cat, fill=plot_cat)) +
      #geom_bar(aes(y=(..count..)/sum(..count..))) +
      #labs(x=colorLab, y="Percent of Sites") +
    ggplot2::geom_bar() +
    ggplot2::labs(x=hist_title, y="Site Count") +
    ggplot2::ggtitle(bquote(atop(.(paste0("Distribution of ", hist_title)),
                          atop(italic(.(plot_subtitle)),"")))) +
    ggplot2::scale_fill_manual(color_label, values=color_breaks, drop=F) +
    ggplot2::theme(plot.title=element_text(hjust = 0.5)) +
    #ggplot2::theme(axis.title.x = element_text(margin = margin(t=10, r=0, b=0, l=0))) +
      #theme(axis.text.x = element_text(size = 8)) + 
      # theme(axis.text.x = element_text(angle = 30, vjust = 1, hjust = 0)) + 
      scale_x_discrete(drop=F) +
      theme_linedraw()  # Add this will not see grey background and white lines
      #theme_minimal()
  if(!is.null(histlim)) gg_hist <- gg_hist + coord_cartesian(ylim=c(0, histlim))
    
  return (list(gg_err, gg_hist))
}