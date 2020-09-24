FUNCTION GET_WDB_OBS_PRECIP, obs_date_YYYYMMDDHH, $
                             duration_hours, $
                             NDV = ndv_, $
                             STATION_OBJ_ID = station_obj_id, $
                             SCRATCH_DIR = scratch_dir

;+
; Adapted from GET_PRECIP_RAW_OBS.
;-
  precip_report = !NULL

;+
; Check arguments for correct type and valid contents.
;-
  if NOT(ISA(obs_date_YYYYMMDDHH, 'STRING')) then begin
      ERR_MSG, 'Start date/time argument must be a STRING.'
      RETURN, snow_depth_report
  endif
  if (STRLEN(obs_date_YYYYMMDDHH) ne 10) then begin
      ERR_MSG, 'Invalid start date/time "' + $
               obs_date_YYYYMMDDHH + $
               '" (required form is YYYYMMDDHH, 10 digits).'
      RETURN, snow_depth_report
  endif
  if NOT(STREGEX(obs_date_YYYYMMDDHH, '[0-9]{10}', /BOOLEAN)) $
      then begin
      ERR_MSG, 'Invalid start date/time "' + $
               obs_date_YYYYMMDDHH + $
               '" (required form is YYYYMMDDHH, all numeric).'
      RETURN, snow_depth_report
  endif

  obs_date_Julian = YYYYMMDDHH_TO_JULIAN(obs_date_YYYYMMDDHH)
  lag = SYSTIME(/JULIAN, /UTC) - obs_date_Julian

  save_file_name = 'wdb0_obs_precip_raw_' + $
                   obs_date_YYYYMMDDHH + $
                   '_' + STRCRA(duration_hours) + 'h' + $
                   '.sav'

  if NOT(KEYWORD_SET(scratch_dir)) then scratch_dir = !NULL
  if ((lag gt 60.0D) and $
      KEYWORD_SET(scratch_dir) and $
      NOT(KEYWORD_SET(station_obj_id))) $
      then begin
      if NOT(FILE_TEST(scratch_dir, /DIRECTORY, /WRITE)) then begin
          ERR_MSG, 'Invalid scratch directory "' + scratch_dir + '".'
          RETURN, !NULL
      endif
      if FILE_TEST(scratch_dir + '/' + save_file_name) then begin
          RESTORE, scratch_dir + '/' + save_file_name
          PRINT, 'Data restored from ' + scratch_dir + '/' + save_file_name
          if (KEYWORD_SET(ndv_) and (ndv_ ne ndv)) then $
              ERR_MSG, 'WARNING: restored file uses ' + $
                       STRCRA(ndv) + $
                       ' for a no-data value and ' + $
                       STRCRA(ndv_) + $
                       ' was specified in this function call.'
          RETURN, precip_report
      endif
  endif

  if KEYWORD_SET(ndv_) then $
      ndv = ndv_ $
  else $
      ndv = -99999.0

;+
; Get precipitation observations.
;-
  obs_date = STRMID(obs_date_YYYYMMDDHH, 0, 4) + '-' + $
             STRMID(obs_date_YYYYMMDDHH, 4, 2) + '-' + $
             STRMID(obs_date_YYYYMMDDHH, 6, 2) + ' ' + $
             STRMID(obs_date_YYYYMMDDHH, 8, 2) + ':00:00'

  duration_sec = duration_hours * 3600L

  if KEYWORD_SET(station_obj_id) then begin

      statement = 'psql -d web_data -h wdb0.dmz.nohrsc.noaa.gov -t -A -c ' + $
                  '"' + $
                  'select ' + $
                  't1.obj_identifier, ' + $
                  'trim(t1.station_id), ' + $
                  'trim(t1.name), ' + $
                  'trim(t1.station_type), ' + $
                  'trim(t1.source), ' + $
                  't1.coordinates[0], ' + $
                  't1.coordinates[1], ' + $
                  't1.elevation, ' + $
                  't1.recorded_elevation, ' + $
                  't2.date, ' + $
                  't2.value ' + $
                  'from point.allstation as t1, ' + $
                  'point.obs_precip_raw as t2 ' + $
                  'where ' + $
                  't2.obj_identifier = ' + STRCRA(station_obj_id) + ' ' + $
                  'and t2.date = ''' + obs_date + ''' ' + $
                  'and t2.duration = ' + STRCRA(duration_sec) + ' ' + $
                  'and t2.value is not NULL ' + $
                  'and t1.obj_identifier = t2.obj_identifier ' + $
                  'order by t2.obj_identifier, t2.date;"'
      
  endif else begin

      statement = 'psql -d web_data -h wdb0.dmz.nohrsc.noaa.gov -t -A -c ' + $
                  '"' + $
                  'select ' + $
                  't1.obj_identifier, ' + $
                  'trim(t1.station_id), ' + $
                  'trim(t1.name), ' + $
                  'trim(t1.station_type), ' + $
                  'trim(t1.source), ' + $
                  't1.coordinates[0], ' + $
                  't1.coordinates[1], ' + $
                  't1.elevation, ' + $
                  't1.recorded_elevation, ' + $
                  't2.date, ' + $
                  't2.value ' + $
                  'from point.allstation as t1, ' + $
                  'point.obs_precip_raw as t2 ' + $
                  'where ' + $
                  't2.date = ''' + obs_date + ''' ' + $
                  'and t2.duration = ' + STRCRA(duration_sec) + ' ' + $
                  'and t2.value is not NULL ' + $
                  'and t1.obj_identifier = t2.obj_identifier ' + $
                  'order by t2.obj_identifier, t2.date;"'

  endelse

  SPAWN, statement, result, EXIT_STATUS = status

  if (status ne 0) then begin
      ERR_MSG, 'psql statement failed: ' + statement
      RETURN, precip_report
  endif

  num_precip = N_ELEMENTS(result)

  if (num_precip eq 0) then begin
      ERR_MSG, 'No reports found.'
      RETURN, precip_report
  endif
  
;+
; Place results in a structure.
;-
  precip_report_ = REPLICATE({station_obj_id: 0L, $
                              station_id: '', $
                              station_name: '', $
                              station_type: '', $
                              station_source: '', $
                              longitude: 0.0D, $
                              latitude: 0.0D, $
                              elevation: 0L, $
                              recorded_elevation: 0L, $
                              date_UTC_Julian: obs_date_Julian, $
                              obs_value_mm: ndv}, $
                             num_precip)

  for rc = 0L, num_precip - 1L do begin

      report = STRSPLIT(result[rc], '|', /EXTRACT)
      if (N_ELEMENTS(report) ne 11) then begin
          ERR_MSG, 'Unrecognized structure in precipitation reports.'
          num_precip = 0
          RETURN, precip_report
      endif

      precip_report_[rc].station_obj_id = LONG(report[0])
      precip_report_[rc].station_id = report[1]
      precip_report_[rc].station_name = report[2]
      precip_report_[rc].station_type = report[3]
      precip_report_[rc].station_source = report[4]
      precip_report_[rc].longitude = DOUBLE(report[5])
      precip_report_[rc].latitude = DOUBLE(report[6])
      precip_report_[rc].elevation = LONG(report[7])
      precip_report_[rc].recorded_elevation = LONG(report[8])
      precip_report_[rc].date_UTC_Julian = DATE_TO_JULIAN(report[9])
      precip_report_[rc].obs_value_mm = FLOAT(report[10]) * 1000.0

  endfor

  if KEYWORD_SET(station_obj_id) then $
      precip_report_ = precip_report_[0]

;+
; Rename the output structure to indicate this procedure has succeeded.
;-
  precip_report = TEMPORARY(precip_report_)

  if ((lag gt 60.0D) and $
      KEYWORD_SET(scratch_dir) and $
      NOT(KEYWORD_SET(station_obj_id))) $
  then begin
      SAVE, precip_report, ndv, $
            FILE = scratch_dir + '/' + save_file_name
      PRINT, 'Data saved to ' + scratch_dir + '/' + save_file_name
  endif

  RETURN, precip_report

end
