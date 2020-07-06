; Examine results of quality control on snow depth observations.

; This program looks at snow depth observations on and around a
; specific time and displays those observations and their QC
; information on a map.

FUNCTION GET_WDB_SNOW_DEPTH_OBS, target_obs_date_YYYYMMDDHH, $
                                 window_start_date_YYYYMMDDHH, $
                                 window_finish_date_YYYYMMDDHH, $
                                 min_lon_, max_lon_, $
                                 min_lat_, max_lat_

; Get hourly snow depth observations from the "web_data" database on
; wdb0.

  snow_depth_report = !NULL


; Check arguments for correct type and valid contents.

  if NOT(ISA(target_obs_date_YYYYMMDDHH, 'STRING')) then begin
      ERR_MSG, 'Target observation date/time argument must be a STRING.'
      RETURN, snow_depth_report
  endif
  if (STRLEN(target_obs_date_YYYYMMDDHH) ne 10) then begin
      ERR_MSG, 'Invalid target observation date/time "' + $
               target_obs_date_YYYYMMDDHH + $
               '" (required form is YYYYMMDDHH, 10 digits).'
      RETURN, snow_depth_report
  endif
  if NOT(STREGEX(target_obs_date_YYYYMMDDHH, '[0-9]{10}', /BOOLEAN)) $
      then begin
      ERR_MSG, 'Invalid target observation date/time "' + $
               target_obs_date_YYYYMMDDHH + $
               '" (required form is YYYYMMDDHH, all numeric).'
      RETURN, snow_depth_report
  endif

  if NOT(ISA(window_start_date_YYYYMMDDHH, 'STRING')) then begin
      ERR_MSG, 'Observation window start date/time argument must be a STRING.'
      RETURN, snow_depth_report
  endif
  if (STRLEN(window_start_date_YYYYMMDDHH) ne 10) then begin
      ERR_MSG, 'Invalid observation window start date/time "' + $
               window_start_date_YYYYMMDDHH + $
               '" (required form is YYYYMMDDHH, 10 digits).'
      RETURN, snow_depth_report
  endif
  if NOT(STREGEX(window_start_date_YYYYMMDDHH, '[0-9]{10}', /BOOLEAN)) $
      then begin
      ERR_MSG, 'Invalid observation window start date/time "' + $
               window_start_date_YYYYMMDDHH + $
               '" (required form is YYYYMMDDHH, all numeric).'
      RETURN, snow_depth_report
  endif

  if NOT(ISA(window_finish_date_YYYYMMDDHH, 'STRING')) then begin
      ERR_MSG, 'Observation window finish date/time argument must be a STRING.'
      RETURN, snow_depth_report
  endif
  if (STRLEN(window_finish_date_YYYYMMDDHH) ne 10) then begin
      ERR_MSG, 'Invalid observation window finish date/time "' + $
               window_finish_date_YYYYMMDDHH + $
               '" (required form is YYYYMMDDHH, 10 digits).'
      RETURN, snow_depth_report
  endif
  if NOT(STREGEX(window_finish_date_YYYYMMDDHH, '[0-9]{10}', /BOOLEAN)) $
      then begin
      ERR_MSG, 'Invalid observation window finish date/time "' + $
               window_finish_date_YYYYMMDDHH + $
               '" (required form is YYYYMMDDHH, all numeric).'
      RETURN, snow_depth_report
  endif

  target_obs_date_Julian = YYYYMMDDHH_TO_JULIAN(target_obs_date_YYYYMMDDHH)
  window_start_date_Julian = YYYYMMDDHH_TO_JULIAN(window_start_date_YYYYMMDDHH)
  window_finish_date_Julian = $
      YYYYMMDDHH_TO_JULIAN(window_finish_date_YYYYMMDDHH)

  if (window_start_date_Julian gt target_obs_date_Julian) then begin
      ERR_MSG, 'Observation window start date/time may not be later than ' + $
               'target observation date/time.'
      RETURN, snow_depth_report
  endif

  if (window_finish_date_Julian lt target_obs_date_Julian) then begin
      ERR_MSG, 'Observation window finish date/time may not be earlier ' + $
               'than target observation date/time.'
      RETURN, snow_depth_report
  endif

  if NOT(ISA(min_lon_, 'DOUBLE')) then $
      min_lon = DOUBLE(min_lon_) $
  else $
      min_lon = min_lon_

  if NOT(ISA(max_lon_, 'DOUBLE')) then $
      max_lon = DOUBLE(max_lon_) $
  else $
      max_lon = max_lon_

  if NOT(ISA(min_lat_, 'DOUBLE')) then $
      min_lat = DOUBLE(min_lat_) $
  else $
      min_lat = min_lat_

  if NOT(ISA(max_lat_, 'DOUBLE')) then $
      max_lat = DOUBLE(max_lat_) $
  else $
      max_lat = max_lat_

; Assemble the IDL save file name.
  minus_hrs = ROUND((target_obs_date_Julian - $
                     window_start_date_Julian) * 24.0D)
  plus_hrs = ROUND((window_finish_date_Julian - $
                    target_obs_date_Julian) * 24.0D)
  foo = FORMAT_FLOAT(min_lon)
  if (STRMID(foo, 0, 1) eq '-') then $
      min_lon_file_str = STRMID(foo, 1) + 'w' $
  else $
      min_lon_file_str = foo + 'e'
  foo = FORMAT_FLOAT(max_lon)
  if (STRMID(foo, 0, 1) eq '-') then $
      max_lon_file_str = STRMID(foo, 1) + 'w' $
  else $
      max_lon_file_str = foo + 'e'
  foo = FORMAT_FLOAT(min_lat)
  if (STRMID(foo, 0, 1) eq '-') then $
      min_lat_file_str = STRMID(foo, 1) + 's' $
  else $
      min_lat_file_str = foo + 'n'
  foo = FORMAT_FLOAT(max_lat)
  if (STRMID(foo, 0, 1) eq '-') then $
      max_lat_file_str = STRMID(foo, 1) + 's' $
  else $
      max_lat_file_str = foo + 'n'
  savFile = 'wdb0_obs_snow_depth' + $
            '_' + target_obs_date_YYYYMMDDHH + $
            '_m' + STRCRA(minus_hrs) + 'h' + $
            '_p' + STRCRA(plus_hrs) + 'h' + $
            '_' + min_lon_file_str + '_to_' + max_lon_file_str + $
            '_' + min_lat_file_str + '_to_' + max_lat_file_str + $
            '.sav'
  savDir = '/net/scratch/nwm_snow_da/wdb0_sav'
  if FILE_TEST(savDir + '/' + savFile) then begin
      RESTORE, savDir + '/' + savFile
      RETURN, snow_depth_report
  endif

  webPGHost = 'wdb0.dmz.nohrsc.noaa.gov'

  
; Generate strings for SQL statements defining the analysis domain.

  min_lon_str = STRCRA(STRING(min_lon, FORMAT='(F25.17)'))
  max_lon_str = STRCRA(STRING(max_lon, FORMAT='(F25.17)'))
  min_lat_str = STRCRA(STRING(min_lat, FORMAT='(F25.17)'))
  max_lat_str = STRCRA(STRING(max_lat, FORMAT='(F25.17)'))


; Get snowfall_raw observations.

  window_start_date = STRMID(window_start_date_YYYYMMDDHH, 0, 4) + '-' + $
                      STRMID(window_start_date_YYYYMMDDHH, 4, 2) + '-' + $
                      STRMID(window_start_date_YYYYMMDDHH, 6, 2) + ' ' + $
                      STRMID(window_start_date_YYYYMMDDHH, 8, 2) + ':' + $
                      '00:00'

  window_finish_date = STRMID(window_finish_date_YYYYMMDDHH, 0, 4) + '-' + $
                       STRMID(window_finish_date_YYYYMMDDHH, 4, 2) + '-' + $
                       STRMID(window_finish_date_YYYYMMDDHH, 6, 2) + ' ' + $
                       STRMID(window_finish_date_YYYYMMDDHH, 8, 2) + ':' + $
                       '00:00'

  statement = 'psql -d web_data -h ' + webPGHost + ' -t -A -c ' + $
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
              't2.value, ' + $
              't3.value_sm_snow_thickness ' + $
              'from point.allstation as t1, ' + $
              'point.obs_snow_depth as t2, ' + $
              'point.rasters_sm as t3 ' + $
              'where ' + $
              't1.coordinates[0] >= ' + min_lon_str + ' ' + $
              'and t1.coordinates[0] <= ' + max_lon_str + ' ' + $
              'and t1.coordinates[1] >= ' + min_lat_str + ' ' + $
              'and t1.coordinates[1] <= ' + max_lat_str + ' ' + $ 
              'and t2.date >= ''' + window_start_date + ''' ' + $
              'and t2.date <= ''' + window_finish_date + ''' ' + $
              'and t2.value is not NULL ' + $
              'and t3.date >= ''' + window_start_date + ''' ' + $
              'and t3.date <= ''' + window_finish_date + ''' ' + $
              'and t1.obj_identifier = t2.obj_identifier ' + $
              'and t1.obj_identifier = t3.obj_identifier ' + $
              'and t3.date = t2.date ' + $
              'order by t1.obj_identifier;"'

;  PRINT, statement
  SPAWN, statement, result, EXIT_STATUS = status

  if (status ne 0) then begin
      ERR_MSG, 'psql statement failed: ' + statement
      RETURN, snow_depth_report
  endif

  num_snow_depth = N_ELEMENTS(result)

  if (result[0] eq '') then begin
      num_snow_depth = 0
      RETURN, snow_depth_report
  endif

; Place results in a structure.

  ndv = -99999.0
  snow_depth_report_ = REPLICATE({station_obj_id: 0L, $
                                  station_id: '', $
                                  station_name: '', $
                                  station_type: '', $
                                  station_source: '', $
                                  longitude: 0.0d, $
                                  latitude: 0.0d, $
                                  elevation: 0L, $
                                  recorded_elevation: 0L, $
                                  date_UTC: '', $
                                  obs_value_cm: ndv, $
                                  mdl_value_cm: ndv}, $
                                 num_snow_depth)

  for sc = 0, num_snow_depth - 1 do begin
      report = STRSPLIT(result[sc], '|', /EXTRACT, /PRESERVE_NULL)
      if (N_ELEMENTS(report) ne 12) then begin
          ERR_MSG, 'Unrecognized structure in snow depth reports.'
          num_snow_depth = 0
          RETURN, snow_depth_report
      endif
      snow_depth_report_[sc].station_obj_id = LONG(report[0])
      snow_depth_report_[sc].station_id = report[1]
      snow_depth_report_[sc].station_name = report[2]
      snow_depth_report_[sc].station_type = report[3]
      snow_depth_report_[sc].station_source = report[4]
      snow_depth_report_[sc].longitude = DOUBLE(report[5])
      snow_depth_report_[sc].latitude = DOUBLE(report[6])
      snow_depth_report_[sc].elevation = LONG(report[7])
      snow_depth_report_[sc].recorded_elevation = LONG(report[8])
      snow_depth_report_[sc].date_UTC = report[9]
      snow_depth_report_[sc].obs_value_cm = FLOAT(report[10]) * 100.0
      if (report[11] ne '') then $
          snow_depth_report_[sc].mdl_value_cm = FLOAT(report[11]) * 100.0
  endfor

; If any stations delivered multiple reports, choose the one closest
; to the analysis time.

  useFlag = BYTARR(num_snow_depth) & useFlag[*] = 1B

  for sc = 0, num_snow_depth - 1 do begin

      ind = WHERE((snow_depth_report_.station_obj_id eq $
                   snow_depth_report_[sc].station_obj_id) and $
                  (useFlag eq 1B), count)
      if (count eq 0) then begin
          ERR_MSG, 'Programming error.'
          RETURN, snow_depth_report
      endif
      if (count eq 1) then CONTINUE
      useFlag[ind] = 0B         ; set all matches to DO NOT USE
      dateList = snow_depth_report_[ind].date_UTC
      dateList_Julian = DBLARR(count)
      for ic = 0, count - 1 do begin
          dateList_Julian[ic] = $
              JULDAY(FIX(STRMID(dateList[ic], 5, 2)), $    ; month
                     FIX(STRMID(dateList[ic], 8, 2)), $    ; day
                     FIX(STRMID(dateList[ic], 0, 4)), $    ; year
                     FIX(STRMID(dateList[ic], 11, 2)), $   ; hour
                     FIX(STRMID(dateLIst[ic], 14, 2)), $   ; minute
                     FIX(STRMID(dateList[ic], 17, 2)))     ; second
      endfor
      timeDiff = MIN(ABS(target_obs_date_Julian - dateList_Julian), minInd)
      useFlag[ind[minInd]] = 1B
  endfor

  ind = WHERE(useFlag eq 1B, num_snow_depth)
  if (num_snow_depth gt 0) then begin
      snow_depth_report_ = snow_depth_report_[ind]
  endif else begin
      ERR_MSG, 'Programming error in unique station search.'
      RETURN, snow_depth_report
  endelse

  snow_depth_report = snow_depth_report_

  SAVE, snow_depth_report, FILENAME = savDir + '/' + savFile

  RETURN, snow_depth_report

end


; MAIN PROGRAM


; Evaluate v0.1.0 of the snow depth QC tests in
; update_station_qc_db.py by using the difference between observed
; snow depth and the snow depth sampled from SNODAS to judge whether
; snow depth observations have been correctly flagged ("hits") or
; incorrectly flagged ("false positives").

; Define the spatial domain.
  min_lon = -134.0D
  max_lon = -60.0D
  min_lat = 20.0D
  max_lat = 58.0D

; Evaluation parameters. These times should be a little after the
; start of the database, and a little before the end of the database,
; to accommodate padding set by num_hrs_pad_prev and
; num_hrs_pad_post.
  start_date_YYYYMMDDHH = '2019100200'
  finish_date_YYYYMMDDHH = '2020060100'

  finish_date_Julian = YYYYMMDDHH_TO_JULIAN(finish_date_YYYYMMDDHH)

; QC data is collected in "clusters", which are closely-grouped
; dates/times over which QC results are gathered together. The
; purposes of this clustering are:
; 1. To assemble enough results so that FAR can be estimated for most
;    or all QC tests.
; 2. To sample a variety of times of day.

  cluster_gap_hours = 6
  cluster_size = 12
  num_hrs_pad_prev = 2
  num_hrs_pad_post = 2
  step_size_days = 6
  full_run = 0

  if ((num_hrs_pad_prev + num_hrs_pad_post) ge cluster_gap_hours) then begin
      ERR_MSG, 'Invalid padding values.'
      STOP
  endif
  sav_file = 'eval_snwd_qc_' + $
             start_date_YYYYMMDDHH + '_to_' + finish_date_YYYYMMDDHH + '.sav'
  if NOT(full_run) then begin
      RESTORE, sav_file
      full_run = 0
      GOTO, SKIP
  endif

; Identify the QC database.
  qcdb_dir = '/net/scratch/fall/m1_dev'
  qcdb_date_range = '2019100100_to_2020093023'
  qcdb_file = 'station_qc_db_' + qcdb_date_range + '.nc.0528'
  qcdb_path = qcdb_dir + '/' + qcdb_file
  if NOT(FILE_TEST(qcdb_path)) then STOP

; Odds and ends.
  ndv = -99999.0
  generate_psv_files = 0
  abs_diff_thresh_cm = [16.0, 122.0]

; Generate a name for an IDL save file.
;  savFile = 

  if generate_psv_files then begin
      login_info = GET_LOGIN_INFO()
      psv_dir = '/net/scratch/' + login_info.user_name
      if NOT(FILE_TEST(psv_dir, /DIRECTORY)) then STOP
  endif

; Open the QC database.
  id = NCDF_OPEN(qcdb_path)

; Check QC database time range against evaluation time period.
  NCDF_ATTGET, id, 'last_datetime_updated', last_datetime_updated, /GLOBAL
  if STRMID(last_datetime_updated, 9, 10, /REVERSE_OFFSET) ne $
      ':00:00 UTC' then STOP
  last_datetime_updated_Julian = $
      GISRS_DATE_TO_JULIAN(STRMID(last_datetime_updated, 0, 13))

  if last_datetime_updated_Julian lt finish_date_Julian then begin
      PRINT, 'Cannot evaluate QC database ' + qcdb_file + $
             ' to ' + JULIAN_TO_GISRS_DATE(finish_date_Julian)
      PRINT, 'QC database only updated to ' + last_datetime_updated
      STOP
  endif

; Get QC flag information from the QC database.
  NCDF_ATTGET, id, 'snow_depth_qc', 'qc_test_names', qc_test_names
  qc_test_names = STRING(qc_test_names)
  NCDF_ATTGET, id, 'snow_depth_qc', 'qc_test_bits', qc_test_bits
  if (N_ELEMENTS(qc_test_names) ne N_ELEMENTS(qc_test_bits)) then STOP

; Generate a pipe-delimited string listing the relevant QC tests.
  qc_test_names_str = ''
  for tc = 0, N_ELEMENTS(qc_test_names) - 1 do begin
      if (qc_test_names[tc] eq 'naught') then CONTINUE
      if (qc_test_names[tc] eq 'anomaly') then CONTINUE
      if (qc_test_names[tc] eq 'rate') then CONTINUE
      if (qc_test_names_str ne '') then $
          qc_test_names_str = qc_test_names_str + '|' + qc_test_names[tc] $
      else $
          qc_test_names_str = qc_test_names[tc]
  endfor

; Get station variables from the QC database.
  NCDF_VARGET, id, 'station_obj_identifier', qcdb_station_obj_id
  NCDF_VARGET, id, 'station_id', qcdb_station_id
  NCDF_VARGET, id, 'station_longitude', qcdb_station_longitude
  NCDF_VARGET, id, 'station_latitude', qcdb_station_latitude
  qcdb_num_stations = N_ELEMENTS(qcdb_station_obj_id)

; Get time variables from the QC database.
  NCDF_VARGET, id, 'time', qcdb_time
  NCDF_ATTGET, id, 'time', 'units', qcdb_time_units
  qcdb_time_units = STRING(qcdb_time_units)
  if (qcdb_time_units ne 'hours since 1970-01-01 00:00:00 UTC') then STOP
  qcdb_time_base_Julian = JULDAY(1, 1, 1970, 0, 0, 0)

; Initialize false alarm ratios.
  FAR = []
  pFAR = []
  solo_freq = []
  num_flagged_obs = []
  solo_FAR = []
  solo_pFAR = []
  num_solo_flagged_obs = []
  cluster_mean_date_Julian = []

; Loop over all clusters.

  cluster_start_date_Julian = YYYYMMDDHH_TO_JULIAN(start_date_YYYYMMDDHH)

  while cluster_start_date_Julian le $
        (finish_date_Julian - $
         cluster_size * cluster_gap_hours  / 24.0D) do begin

      PRINT, 'Cluster start date: ' + $
             JULIAN_TO_YYYYMMDDHH(cluster_start_date_Julian)

      date_Julian = cluster_start_date_Julian
      
;     Initialize (erase) the inventory of how many times each QC test
;     flags an observation within the current cluster
      inventory = !NULL
      hit_count = 0L ; hits
      fa_count = 0L ; false alarms
      pfa_count = 0L ; possible false alarms

      date_total = 0.0D

      for cc = 0, cluster_size - 1 do begin

          date_total = date_total + date_Julian
          eval_target_YYYYMMDDHH = JULIAN_TO_YYYYMMDDHH(date_Julian)

          PRINT, '  Get QC data for ' + eval_target_YYYYMMDDHH

          if generate_psv_files then begin
              psv_file_flagged = psv_file_dir + '/' + $
                                 'SNWD_QC_' + eval_target_YYYYMMDDHH + $
                                 '_flagged.txt'
              psv_file_unflagged = psv_file_dir + '/' + $
                                   'SNWD_QC_' + eval_target_YYYYMMDDHH + $
                                   '_unflagged.txt'
              OPENW, lunf, psv_file_flagged, /GET_LUN
              OPENW, lunu, psv_file_unflagged, /GET_LUN

              PRINTF, lunf, $
                      'longitude|latitude|' + qc_test_names_str + $
                      '|date|station_id|' + $
                      'station_type|station_source|' + $
                      'elevation|rec_elevation|' + $
                      'obs_snow_depth_cm|mdl_snow_depth_cm|' + $
                      'delta_snow_depth_cm'
              PRINTF, lunu, $
                      'longitude|latitude|' + qc_test_names_str + $
                      '|date|station_id|' + $
                      'station_type|station_source|' + $
                      'elevation|rec_elevation|' + $
                      'obs_snow_depth_cm|mdl_snow_depth_cm|' + $
                      'delta_snow_depth_cm'
          endif

;         Calculate start/finish times for the narrow period of hours
;         that can be roughly attributed to eval_target_YYYYMMDDHH.
          eval_target_Julian = YYYYMMDDHH_TO_JULIAN(eval_target_YYYYMMDDHH)
          eval_start_Julian = eval_target_Julian - $
                              DOUBLE(num_hrs_pad_prev) / 24.0D
          eval_start_YYYYMMDDHH = JULIAN_TO_YYYYMMDDHH(eval_start_Julian)
          eval_finish_Julian = eval_target_Julian + $
                               DOUBLE(num_hrs_pad_prev) / 24.0D
          eval_finish_YYYYMMDDHH = JULIAN_TO_YYYYMMDDHH(eval_finish_Julian)

;         Get snow depth observations.
          wdb_snow_depth_obs = $
              GET_WDB_SNOW_DEPTH_OBS(eval_target_YYYYMMDDHH, $
                                     eval_start_YYYYMMDDHH, $
                                     eval_finish_YYYYMMDDHH, $
                                     min_lon, max_lon, $
                                     min_lat, max_lat)

          PRINT, '  Found ' + STRCRA(N_ELEMENTS(wdb_snow_depth_obs)) + $
                 ' snow depth obs.'

;         Determine QC database time indices for these observations.
          eval_start_qcdb = $
              ROUND((eval_start_Julian - qcdb_time_base_Julian) * 24)
          qcdb_t1_1 = WHERE(qcdb_time eq eval_start_qcdb, count)
          if (count ne 1) then STOP
          qcdb_t1_1 = qcdb_t1_1[0]

          eval_finish_qcdb = $
              ROUND((eval_finish_Julian - qcdb_time_base_Julian) * 24)
          qcdb_t1_2 = WHERE(qcdb_time eq eval_finish_qcdb, count)
          if (count ne 1) then STOP
          qcdb_t1_2 = qcdb_t1_2[0]

          if ((qcdb_t1_2 - qcdb_t1_1) ne $
              (num_hrs_pad_prev + num_hrs_pad_post)) then STOP

;         Get QC data for snow depth observations.
          NCDF_VARGET, id, 'snow_depth_qc', snwd_qc, $
                       COUNT = [num_hrs_pad_prev + num_hrs_pad_post + 1, $
                                qcdb_num_stations], $
                       OFFSET = [qcdb_t1_1, 0]
          NCDF_VARGET, id, 'snow_depth_qc_checked', snwd_qc_checked, $
                       COUNT = [num_hrs_pad_prev + num_hrs_pad_post + 1, $
                                qcdb_num_stations], $
                       OFFSET = [qcdb_t1_1, 0]

;         Determine the QC data time index for each snow depth
;         observation.
          obs_date = wdb_snow_depth_obs.date_UTC
          wdb_date_Julian = $
              JULDAY(FIX(STRMID(obs_date, 5, 2)), $
                     FIX(STRMID(obs_date, 8, 2)), $
                     FIX(STRMID(obs_date, 0, 4)), $
                     FIX(STRMID(obs_date, 11, 2)), $
                     FIX(STRMID(obs_date, 14, 2)), $
                     FIX(STRMID(obs_date, 17, 2)))
          wdb_date_hours = $
              ROUND((wdb_date_Julian - qcdb_time_base_Julian) * 24.0)
          wdb_date_Julian = !NULL
          obs_date = !NULL

;         Sort data in reverse order of the absolute model - obs
;         difference.
          abs_diff = ABS(wdb_snow_depth_obs.mdl_value_cm - $
                         wdb_snow_depth_obs.obs_value_cm)
          order = REVERSE(SORT(abs_diff))

          abs_diff = abs_diff[order]
          wdb_snow_depth_obs = wdb_snow_depth_obs[order]
          wdb_date_hours = wdb_date_hours[order]

          for wdb_si = 0, N_ELEMENTS(wdb_snow_depth_obs) - 1 do begin

              if ((wdb_snow_depth_obs[wdb_si].mdl_value_cm eq ndv) or $
                  (wdb_snow_depth_obs[wdb_si].obs_value_cm eq ndv)) then begin
                  ERR_MSG, 'WARNING: STATION "' + $
                           wdb_snow_depth_obs[wdb_si].station_id + $
                           '" has missing values; skipping.'
                  continue
              endif
                           
;             Locate the position of this station in the QC database.
              qcdb_si = WHERE(qcdb_station_obj_id eq $
                              wdb_snow_depth_obs[wdb_si].station_obj_id, $
                              count)
              if (count eq 0) then begin
                  ERR_MSG, 'WARNING: station "' + $
                           wdb_snow_depth_obs[wdb_si].station_id + $
                           '" not found in QC database; skipping.'
                  continue
              endif
              if (count ne 1) then STOP
              qcdb_si = qcdb_si[0]

;             Get the time index of this observation.
              qcdb_ti = wdb_date_hours[wdb_si] - eval_start_qcdb

;             Generate a few strings for convenience.
              obs_str = STRCRA(wdb_snow_depth_obs[wdb_si].obs_value_cm)
              site_str = STRCRA(wdb_snow_depth_obs[wdb_si].station_id)
              time_str = STRCRA(wdb_snow_depth_obs[wdb_si].date_UTC)

              ;; if (wdb_si eq 126) then begin
              ;;     PRINT, snwd_qc_checked[qcdb_ti, qcdb_si]
              ;;     PRINT, snwd_qc[qcdb_ti, qcdb_si]
              ;;     HELP, wdb_snow_depth_obs[wdb_si], /STRUCT
              ;;     PRINT, abs_diff[wdb_si]
              ;;     STOP
              ;; endif

              if (snwd_qc_checked[qcdb_ti, qcdb_si] ne 0) then begin

;                 Decode QC value.
                  qc_str = ''
                  qc_val = snwd_qc[qcdb_ti, qcdb_si]
                  qc_val_str = STRING(qc_val, FORMAT = '(B)')

;                 Replace whitespace padding in qc_val_str with zero padding
;                 for higher bits.
                  STR_REPLACE, qc_val_str, ' ', '0'

;                 Initialize inventories for flags.
                  if NOT(ISA(inventory)) then begin
                  ; Count QC tests and initialize test inventories.
                      num_qc_tests = 0
                      for tc = 0, N_ELEMENTS(qc_test_names) - 1 do begin
                          if (qc_test_names[tc] eq 'naught') then CONTINUE
                          if (qc_test_names[tc] eq 'anomaly') then CONTINUE
                          if (qc_test_names[tc] eq 'rate') then CONTINUE
                          num_qc_tests++
                      endfor
                      inventory = LONARR(num_qc_tests)
                      ;; inventory_multiple = LONARR(num_qc_tests)
                      inventory_fa = LONARR(num_qc_tests)
                      inventory_pfa = LONARR(num_qc_tests)
                      inventory_hit = LONARR(num_qc_tests)
                      solo_inventory = LONARR(num_qc_tests)
                      solo_inventory_fa = LONARR(num_qc_tests)
                      solo_inventory_pfa = LONARR(num_qc_tests)
                      solo_inventory_hit = LONARR(num_qc_tests)
                      count_fa = 0
                      count_pfa = 0
                      count_hit = 0
                  endif
                  inv_ind = 0
                  this_inventory = LONARR(num_qc_tests)
                  for tc = 0, N_ELEMENTS(qc_test_names) - 1 do begin
                      if (qc_test_names[tc] eq 'naught') then CONTINUE
                      if (qc_test_names[tc] eq 'anomaly') then CONTINUE
                      if (qc_test_names[tc] eq 'rate') then CONTINUE
                      qc_bit_str = STRMID(qc_val_str, qc_test_bits[tc], 1, $
                                          /REVERSE_OFFSET)
                      if (qc_str ne '') then $
                          qc_str = qc_str + '|' + qc_bit_str $
                      else $
                          qc_str = qc_bit_str
                      if (qc_bit_str eq '1') then $
                          this_inventory[inv_ind] = this_inventory[inv_ind] + 1
                      inv_ind++
                  endfor
                  
;                 Identify "solo" flags. If any test is the only one
;                 to flag a report, note this using solo_ti and
;                 solo_inventory.

                  solo_ti = -1
                  for tc = 0, num_qc_tests - 1 do begin
                      if ((this_inventory[tc] eq 1) and $
                          (TOTAL(this_inventory) eq 1)) then begin
                          solo_ti = tc
                          solo_inventory[tc] = solo_inventory[tc] + 1
                      endif
                  endfor

;                 Increment the full inventory
                  inventory = inventory + this_inventory

                  if (snwd_qc[qcdb_ti, qcdb_si] ne 0) then begin
                      
;                     This observation has been flagged by at least
;                     one test. Use abs_diff to decide if this flag is
;                     a false alarm, possible false alarm, or hit. A
;                     "hit" means that the observation has been
;                     properly flagged.

;                     Judge the QC flag/s based on the difference
;                     between the observed and modeld (SNODAS) snow
;                     depth.

;                     If any 

                      ;; PRINT, 'Snow depth value ' + obs_str + ' ' + $
                      ;;        'at station "' + site_str + '" ' + $
                      ;;        'flagged with value ' + $
                      ;;        STRCRA(snwd_qc[qcdb_ti, qcdb_si])

                      case 1 of
                          (abs_diff[wdb_si] ge 0.0) and $
                              (abs_diff[wdb_si] lt abs_diff_thresh_cm[0]): begin
                              ; false alarm
                              inventory_fa = inventory_fa + this_inventory
                              if (solo_ti ne -1) then $
                                  solo_inventory_fa[solo_ti] = $
                                  solo_inventory_fa[solo_ti] + 1
                              count_fa++
                          end
                          (abs_diff[wdb_si] ge abs_diff_thresh_cm[0]) and $
                              (abs_diff[wdb_si] lt abs_diff_thresh_cm[1]): begin
                              ; possible false alarm
                              inventory_pfa = inventory_pfa + this_inventory
                              if (solo_ti ne -1) then $
                                  solo_inventory_pfa[solo_ti] = $
                                  solo_inventory_pfa[solo_ti] + 1
                              count_pfa++
                          end
                          else: begin
                              ; hit
                              inventory_hit = inventory_hit + this_inventory
                              if (solo_ti ne -1) then $
                                  solo_inventory_hit[solo_ti] = $
                                  solo_inventory_hit[solo_ti] + 1
                              count_hit++
                          end
                      endcase

                  endif

              endif

          endfor

          date_Julian = date_Julian + DOUBLE(cluster_gap_hours) / 24.0D

      endfor

      cluster_mean_date_Julian = [cluster_Mean_date_Julian, $
                                  date_total / cluster_size]

      inventory_multiple = inventory - solo_inventory
      ;; inventory_solo = inventory - inventory_multiple
      ;; print, inventory_solo
      ;; print, solo_inventory
      ;; print, 'hmmm'
      ;; print, inventory
      ;; ;; print, inventory_multiple
      ;; ;; print, inventory_solo
      ;; print, inventory_fa
      ;; print, inventory_pfa
      ;; print, inventory_hit
      ;; print, total(inventory_fa), count_fa
      ;; print, total(inventory_pfa), count_pfa
      ;; print, total(inventory_hit), count_hit

      cluster_FAR = FLOAT(inventory_fa) / FLOAT(inventory)
      cluster_pFAR = FLOAT(inventory_fa + inventory_pfa) / FLOAT(inventory)
      cluster_solo_freq = FLOAT(solo_inventory) / FLOAT(inventory)
      ind = WHERE(inventory eq 0, count)
      if (count gt 0) then begin
          cluster_FAR[ind] = -1.0
          cluster_pFAR[ind] = -1.0
          cluster_solo_freq[ind] = -1.0
      endif

      cluster_solo_FAR = FLOAT(solo_inventory_fa) / FLOAT(solo_inventory)
      cluster_solo_pFAR = FLOAT(solo_inventory_fa + solo_inventory_pfa) / $
                          FLOAT(solo_inventory)
      ind = WHERE(solo_inventory eq 0, count)
      if (count gt 0) then begin
          cluster_solo_FAR[ind] = -1.0
          cluster_solo_pFAR[ind] = -1.0
      endif

      FAR = [FAR, TRANSPOSE(cluster_FAR)]
      pFAR = [pFAR, TRANSPOSE(cluster_pFAR)]
      solo_freq = [solo_freq, TRANSPOSE(cluster_solo_freq)]
      num_flagged_obs = [num_flagged_obs, TRANSPOSE(inventory)]

      solo_FAR = [solo_FAR, TRANSPOSE(cluster_solo_FAR)]
      solo_pFAR = [solo_pFAR, TRANSPOSE(cluster_solo_pFAR)]
      num_solo_flagged_obs = [num_solo_flagged_obs, TRANSPOSE(solo_inventory)]

      cluster_start_date_Julian = date_Julian + DOUBLE(step_size_days)

  endwhile


SKIP:
  if full_run then SAVE, /ALL, FILENAME = sav_file


; For plotting.
  char_size = 1.0
  sym_size = 1.0
  other_col = 80
  fill_col = 220
  far_sym = -5
  pfar_sym = -8
  pos = [0.1, 0.25, 0.9, 0.75]
  USERSYM, [-1, 1, 0, -1], [1, 1, -1, 1], THICK = 2

; Show a time series of all false alarm ratios.

  ti = 0

  for tc = 0, N_ELEMENTS(qc_test_names) - 1 do begin

      if (qc_test_names[tc] eq 'naught') then CONTINUE
      if (qc_test_names[tc] eq 'anomaly') then CONTINUE
      if (qc_test_names[tc] eq 'rate') then CONTINUE

      dummy = LABEL_DATE(DATE_FORMAT = '%Y!C%M-%D')

      ind = WHERE((pFar[*, ti] ne -1.0) and $
                  (FAR[*, ti] ne -1.0) and $
                  (solo_freq[*, ti] ne -1.0), count)
      if (count eq 0) then begin
          PRINT, 'No results for "' + qc_test_names[tc] + '" test'
          ti++
          CONTINUE
      endif

      TVLCT, red, grn, blu, /GET
      red[other_col] = 150
      grn[other_col] = 0
      blu[other_col] = 0

      SET_PLOT, 'PS'
      plot_file = 'eval_snwd_qc' + $
                  '_' + start_date_YYYYMMDDHH + '_to_' + $
                  finish_date_YYYYMMDDHH + $
                  '_' + qc_test_names[tc]
      DEVICE, /COLOR, FILE = plot_file + '.ps'
      TVLCT, red, grn, blu
      !P.Font = 1 ; TrueType
      DEVICE, SET_FONT = 'DejaVuSans', /TT_FONT

      PLOT, cluster_mean_date_Julian[ind], $
            pFAR[ind, ti], $
            THICK = 2, XTHICK = 2, YTHICK = 2, CHARTHICK = 2, $
            YRANGE = [0.0, 1.0], $
            ;; PSYM = pfar_sym, $
            TITLE = 'FAR for v0.1.0 "' + qc_test_names[tc] + '" test!C ', $
            XTICKFORMAT = 'LABEL_DATE', XTICKUNITS = 'Time', $
            YTITLE = 'False Alarm Ratio', $
            POS  = pos, $
            YSTYLE = 8, $
            CHARSIZE = char_size, $
            /NODATA
;            SYMSIZE = sym_size, $
;            /NOCLIP

      POLYFILL, [cluster_mean_date_Julian[ind[0]], $
                 cluster_mean_date_Julian[ind], $
                 REVERSE(cluster_mean_date_Julian[ind])], $
                [FAR[ind[0], ti], $
                 pFAR[ind, ti], $
                 REVERSE(FAR[ind, ti])], $
                COLOR = fill_col

      OPLOT, cluster_mean_date_Julian[ind], pFAR[ind, ti], $
             PSYM = pfar_sym, THICK = 2, SYMSIZE = sym_size, /NOCLIP

      OPLOT, cluster_mean_date_Julian[ind], FAR[ind, ti], $
             PSYM = far_sym, THICK = 2, SYMSIZE = sym_size, /NOCLIP

      PLOT, cluster_mean_date_Julian, $
            num_flagged_obs[*, ti], $
            POS = pos, $
            XTICKUNITS = 'Time', $
            XSTYLE = 4, YSTYLE = 4, $
            /NODATA, /NOERASE

      OPLOT, cluster_mean_date_Julian, $
             num_flagged_obs[*, ti], $
             THICK = 2, LINESTYLE = 2, COLOR = other_col, /NOCLIP

      AXIS, YAXIS = 1, YTITLE = '# Flagged', CHARSIZE = char_size, $
            YTHICK = 2, CHARTHICK = 2, COLOR = other_col

;     Legend.
      x1Leg = 0.62
      x2Leg = 0.72
      yLeg = 0.70
      yNudge = 0.01
      xBreak = 0.02
      yBreak = 0.05
      POLYFILL, [x1Leg, x1Leg, x2Leg, x2Leg, x1Leg], $
                [yLeg - yBreak, yLeg, yLeg, yLeg - yBreak, yLeg - yBreak], $
                COLOR = fill_col, /NORMAL
      PLOTS, [x1Leg, x2Leg], [yLeg, yLeg], /NORMAL, $
             PSYM = pfar_sym, SYMSIZE = sym_size, THICK = 2
      XYOUTS, x2Leg + xBreak, yLeg - yNudge, 'Possible FAR', /NORMAL, $
              CHARSIZE = char_size, CHARTHICK = 2
      yLeg = yLeg - yBreak
      PLOTS, [x1Leg, x2Leg], [yLeg, yLeg], /NORMAL, $
             PSYM = far_sym, SYMSIZE = sym_size, THICK = 2
      XYOUTS, x2Leg + xBreak, yLeg - yNudge, 'Likely FAR', /NORMAL, $
              CHARSIZE = char_size, CHARTHICK = 2
      yLeg = yLeg - yBreak
      PLOTS, [x1Leg, x2Leg], [yLeg, yLeg], /NORMAL, $
             LINESTYLE = 2, COLOR = other_col, THICK = 2
      XYOUTS, x2Leg + xBreak, yLeg - yNudge, '# Flagged', /NORMAL, $
              CHARSIZE = char_size, CHARTHICK = 2, COLOR = other_col

      DEVICE, /CLOSE
      cmd = 'pstopng ' + plot_file + '.ps'
      SPAWN, cmd, EXIT_STATUS = status
      if (status ne 0) then STOP
      cmd = 'mogrify -trim -border 4% -bordercolor white ' + plot_file + '.png'
      SPAWN, cmd, EXIT_STATUS = status
      if (status ne 0) then STOP

      ;; SET_PLOT, 'X'
      ;; WSET_OR_WINDOW, 2
      SET_PLOT, 'PS'
      plot_file = 'eval_snwd_qc' + $
                  '_' + start_date_YYYYMMDDHH + '_to_' + $
                  finish_date_YYYYMMDDHH + $
                  '_' + qc_test_names[tc] + '_solo'
      DEVICE, /COLOR, FILE = plot_file + '.ps'
      TVLCT, red, grn, blu
      !P.Font = 1 ; TrueType
      DEVICE, SET_FONT = 'DejaVuSans', /TT_FONT

      PLOT, cluster_mean_date_Julian[ind], $
            solo_freq[ind, ti], $
            THICK = 2, XTHICK = 2, YTHICK = 2, CHARTHICK = 2, $
            YRANGE = [0.0, 1.0], $
            LINESTYLE = 3, $
;            PSYM = far_sym, $ 
            TITLE = 'Solo Frequency / FAR for v0.1.0 "' + $
                    qc_test_names[tc] + '" test!C ', $
            XTICKFORMAT = 'LABEL_DATE', XTICKUNITS = 'Time', $
            YTITLE = 'Solo Frequency / FAR', $
            POS = pos, $
            YSTYLE = 8, $
            CHARSIZE = char_size, $
            SYMSIZE = sym_size, $
            /NOCLIP

      ; num_solo_flagged_obs / solo_freq = num_flagged_obs

      ind = WHERE((solo_FAR[*, ti] ne -1.0) and $
                  (solo_pFAR[*, ti] ne -1.0), count)
      if (count ne 0) then begin
          POLYFILL, [cluster_mean_date_Julian[ind[0]], $
                     cluster_mean_date_Julian[ind], $
                     REVERSE(cluster_mean_date_Julian[ind])], $
                    [solo_FAR[ind[0], ti], $
                     solo_pFAR[ind, ti], $
                     REVERSE(solo_FAR[ind, ti])], $
                    COLOR = fill_col
          OPLOT, cluster_mean_date_Julian[ind], $
                 solo_FAR[ind, ti], $
                 PSYM = far_sym, THICK = 2, SYMSIZE = sym_size, /NOCLIP
          OPLOT, cluster_mean_date_Julian[ind], $
                 solo_pFAR[ind, ti], $
                 PSYM = pfar_sym, THICK = 2, SYMSIZE = sym_size, /NOCLIP
      endif

      PLOT, cluster_mean_date_Julian, $
            num_solo_flagged_obs[*, ti], $
            POS = pos, $
            XTICKUNITS = 'Time', $
            XSTYLE = 4, YSTYLE = 4, $
            /NODATA, /NOERASE

      OPLOT, cluster_mean_date_Julian, $
             num_solo_flagged_obs[*, ti], $
             THICK = 2, LINESTYLE = 2, COLOR = other_col

      AXIS, YAXIS = 1, YTITLE = '# Solo-Flagged', CHARSIZE = char_size, $
            YTHICK = 2, CHARTHICK = 2, COLOR = other_col

;     Legend
      x1Leg = 0.58
      x2Leg = 0.68
      yLeg = 0.70
      yNudge = 0.01
      xBreak = 0.02
      yBreak = 0.05
      POLYFILL, [x1Leg, x1Leg, x2Leg, x2Leg, x1Leg], $
                [yLeg - yBreak, yLeg, yLeg, yLeg - yBreak, yLeg - yBreak], $
                COLOR = fill_col, /NORMAL
      PLOTS, [x1Leg, x2Leg], [yLeg, yLeg], /NORMAL, $
             PSYM = pfar_sym, SYMSIZE = sym_size, THICK = 2
      XYOUTS, x2Leg + xBreak, yLeg - yNudge, 'Possible Solo FAR', /NORMAL, $
              CHARSIZE = char_size, CHARTHICK = 2
      yLeg = yLeg - yBreak
      PLOTS, [x1Leg, x2Leg], [yLeg, yLeg], /NORMAL, $
             PSYM = far_sym, SYMSIZE = sym_size, THICK = 2
      XYOUTS, x2Leg + xBreak, yLeg - yNudge, 'Likely Solo FAR', /NORMAL, $
              CHARSIZE = char_size, CHARTHICK = 2
      yLeg = yLeg - yBreak
      PLOTS, [x1Leg, x2Leg], [yLeg, yLeg], /NORMAL, $
             LINESTYLE = 3, THICK = 2
      XYOUTS, x2Leg + xBreak, yLeg - yNudge, 'Solo Frequency', /NORMAL, $
              CHARSIZE = char_size, CHARTHICK = 2
      yLeg = yLeg - yBreak
      PLOTS, [x1Leg, x2Leg], [yLeg, yLeg], /NORMAL, $
             LINESTYLE = 2, COLOR = other_col, THICK = 2
      XYOUTS, x2Leg + xBreak, yLeg - yNudge, '# Solo-Flagged', /NORMAL, $
              CHARSIZE = char_size, CHARTHICK = 2, COLOR = other_col

      DEVICE, /CLOSE
      cmd = 'pstopng ' + plot_file + '.ps'
      SPAWN, cmd, EXIT_STATUS = status
      if (status ne 0) then STOP
      cmd = 'mogrify -trim -border 4% -bordercolor white ' + plot_file + '.png'
      SPAWN, cmd, EXIT_STATUS = status
      if (status ne 0) then STOP

      ;; if (tc ne N_ELEMENTS(qc_test_names) - 1) then $
      ;;     move = GET_KBRD(1)

      ti++

  endfor

  if full_run then NCDF_CLOSE, id

end
