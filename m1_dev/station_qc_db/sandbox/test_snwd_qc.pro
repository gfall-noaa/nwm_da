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

  PRINT, statement
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

  RETURN, snow_depth_report

end





















; Identify the QC database.
  ; qcdb_dir = '/net/scratch/fall/m1_dev'
  qcdb_dir = '/net/lfs0data6/nwm_da/m1_dev'
  ; qcdb_date_range = '2019110100_to_2019111023'
  qcdb_date_range = '2020050100_to_2020050223'

  qcdb_file = 'station_qc_db_' + qcdb_date_range + '.nc'
  qcdb_path = qcdb_dir + '/' + qcdb_file

  if NOT(FILE_TEST(qcdb_path)) then STOP

; Set the time range to be evaluated.
  ; eval_target_YYYYMMDDHH = '2019110112'
  eval_target_YYYYMMDDHH = '2020050112'
  num_hrs_pad_prev = 3
  num_hrs_pad_post = 3

; Define the spatial domain.
  min_lon = -130.0D
  max_lon = -60.0D
  min_lat = 24.0D
  max_lat = 50.0D

; Open the QC database.
  id = NCDF_OPEN(qcdb_path)

; Verify that the QC database is updated to cover the evaluation time
; range.
  eval_target_Julian = YYYYMMDDHH_TO_JULIAN(eval_target_YYYYMMDDHH)
  eval_start_Julian = eval_target_Julian - DOUBLE(num_hrs_pad_prev) / 24.0D
  eval_start_YYYYMMDDHH = JULIAN_TO_YYYYMMDDHH(eval_start_Julian)
  eval_finish_Julian = eval_target_Julian + DOUBLE(num_hrs_pad_prev) / 24.0D
  eval_finish_YYYYMMDDHH = JULIAN_TO_YYYYMMDDHH(eval_finish_Julian)

  NCDF_ATTGET, id, 'last_datetime_updated', last_datetime_updated, /GLOBAL
  if STRMID(last_datetime_updated, 9, 10, /REVERSE_OFFSET) ne $
      ':00:00 UTC' then STOP
  last_datetime_updated_Julian = $
      GISRS_DATE_TO_JULIAN(STRMID(last_datetime_updated, 0, 13))

  if last_datetime_updated_Julian lt eval_finish_Julian then begin
      PRINT, 'Cannot evaluate QC database ' + qcdb_file + $
             ' to ' + JULIAN_TO_GISRS_DATE(eval_finish_Julian)
      PRINT, 'QC database only updated to ' + last_datetime_updated
      STOP
  endif

; Get QC flag information from the QC database.
  NCDF_ATTGET, id, 'snow_depth_qc', 'qc_test_names', qc_test_names
  qc_test_names = STRING(qc_test_names)
  NCDF_ATTGET, id, 'snow_depth_qc', 'qc_test_bits', qc_test_bits
  if (N_ELEMENTS(qc_test_names) ne N_ELEMENTS(qc_test_bits)) then STOP

; Get snow depth observations.
  ndv = -99999.0
  wdb_snow_depth_obs = GET_WDB_SNOW_DEPTH_OBS(eval_target_YYYYMMDDHH, $
                                              eval_start_YYYYMMDDHH, $
                                              eval_finish_YYYYMMDDHH, $
                                              min_lon, max_lon, $
                                              min_lat, max_lat)

  PRINT, 'Found ' + STRCRA(N_ELEMENTS(wdb_snow_depth_obs)) + ' snow depth obs.'

; Get QC data for snow depth observations.
  NCDF_VARGET, id, 'snow_depth_qc', snwd_qc
  NCDF_VARGET, id, 'snow_depth_qc_checked', snwd_qc_checked
  NCDF_ATTGET, id, 'snow_depth_qc', 'qc_test_bits', snwd_qc_bits
  NCDF_ATTGET, id, 'snow_depth_qc', 'qc_test_names', snwd_qc_names

  NCDF_VARGET, id, 'station_obj_identifier', qcdb_station_obj_id
  NCDF_VARGET, id, 'station_id', qcdb_station_id
  NCDF_VARGET, id, 'station_longitude', qcdb_station_longitude
  NCDF_VARGET, id, 'station_latitude', qcdb_station_latitude

  NCDF_VARGET, id, 'time', qcdb_time
  NCDF_ATTGET, id, 'time', 'units', qcdb_time_units
  qcdb_time_units = STRING(qcdb_time_units)
  if (qcdb_time_units ne 'hours since 1970-01-01 00:00:00 UTC') then STOP
  qcdb_time_base_Julian = JULDAY(1, 1, 1970, 0, 0, 0)

; Determine the QC database time index for each snow depth observation.
  obs_date = wdb_snow_depth_obs.date_UTC
  wdb_date_Julian = $
      JULDAY(FIX(STRMID(obs_date, 5, 2)), $
             FIX(STRMID(obs_date, 8, 2)), $
             FIX(STRMID(obs_date, 0, 4)), $
             FIX(STRMID(obs_date, 11, 2)), $
             FIX(STRMID(obs_date, 14, 2)), $
             FIX(STRMID(obs_date, 17, 2)))
  help, wdb_date_julian
  wdb_date_hours = ROUND((wdb_date_Julian - qcdb_time_base_Julian) * 24.0)
  obs_date = !NULL

  psv_file_flagged = 'SNWD_QC_' + eval_target_YYYYMMDDHH + '_flagged.txt'
  psv_file_unflagged = 'SNWD_QC_' + eval_target_YYYYMMDDHH + '_unflagged.txt'
  OPENW, lunf, psv_file_flagged, /GET_LUN
  OPENW, lunu, psv_file_unflagged, /GET_LUN

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

  PRINTF, lunf, $
          'longitude|latitude|' + qc_test_names_str + '|date|station_id|' + $
          'station_type|station_source|elevation|rec_elevation|' + $
          'obs_snow_depth_cm|mdl_snow_depth_cm|delta_snow_depth_cm'
  PRINTF, lunu, $
          'longitude|latitude|' + qc_test_names_str + '|date|station_id|' + $
          'station_type|station_source|elevation|rec_elevation|' + $
          'obs_snow_depth_cm|mdl_snow_depth_cm|delta_snow_depth_cm'

  inventory = !NULL

; Sort data in reverse order of the absolute model - obs difference.

  abs_diff = ABS(wdb_snow_depth_obs.mdl_value_cm - $
                 wdb_snow_depth_obs.obs_value_cm)
  order = REVERSE(SORT(abs_diff))

  for wdb_si_ = 0, N_ELEMENTS(wdb_snow_depth_obs) - 1 do begin

      wdb_si = order[wdb_si_]

;     Locate the position of this observation in the QC database.
      qcdb_si = WHERE(qcdb_station_obj_id eq $
                      wdb_snow_depth_obs[wdb_si].station_obj_id, count)
      if (count eq 0) then begin
          ERR_MSG, 'Missing QC data.'
          STOP
      endif
      if (count ne 1) then STOP
      qcdb_si = qcdb_si[0]

      qcdb_ti = WHERE(qcdb_time eq wdb_date_hours[wdb_si])
      qcdb_ti = qcdb_ti[0]

      obs_str = STRCRA(wdb_snow_depth_obs[wdb_si].obs_value_cm)
      site_str = STRCRA(wdb_snow_depth_obs[wdb_si].station_id)
      time_str = STRCRA(wdb_snow_depth_obs[wdb_si].date_UTC)

  ;; snow_depth_report_ = REPLICATE({station_obj_id: 0L, $
  ;;                                 station_id: '', $
  ;;                                 station_name: '', $
  ;;                                 station_type: '', $
  ;;                                 longitude: 0.0d, $
  ;;                                 latitude: 0.0d, $
  ;;                                 elevation: 0L, $
  ;;                                 recorded_elevation: 0L, $
  ;;                                 date_UTC: '', $
  ;;                                 obs_value_cm: 0.0}, $
  ;;                                num_snow_depth)

       if (snwd_qc_checked[qcdb_ti, qcdb_si] ne 0) then begin

;         Decode QC value.
          qc_str = ''
          qc_val = snwd_qc[qcdb_ti, qcdb_si]
          qc_val_str = STRING(qc_val, FORMAT = '(B)')

;         Replace whitespace padding in qc_val_str with zero padding
;         for higher bits.
          STR_REPLACE, qc_val_str, ' ', '0'

;         Initialize inventories for flags.
          if NOT(ISA(inventory)) then begin
              ; Count QC tests.
              num_qc_tests = 0
              for tc = 0, N_ELEMENTS(qc_test_names) - 1 do begin
                  if (qc_test_names[tc] eq 'naught') then CONTINUE
                  if (qc_test_names[tc] eq 'anomaly') then CONTINUE
                  if (qc_test_names[tc] eq 'rate') then CONTINUE
                  num_qc_tests++
              endfor
              inventory = LONARR(num_qc_tests)
              inventory_multiple = LONARR(num_qc_tests)
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

          ; Identify multiple flags.
          for tc = 0, num_qc_tests - 1 do begin
              if ((this_inventory[tc] eq 1) and $
                  (TOTAL(this_inventory) gt 1)) then $
                      inventory_multiple[tc] = inventory_multiple[tc] + 1
          endfor

          ; Increment the full inventory.
          inventory = inventory + this_inventory

          delta_snwd_cm = ndv
          if ((wdb_snow_depth_obs[wdb_si].obs_value_cm ne ndv) and $
              (wdb_snow_depth_obs[wdb_si].mdl_value_cm ne ndv)) then $
                  delta_snwd_cm = wdb_snow_depth_obs[wdb_si].mdl_value_cm - $
                                  wdb_snow_depth_obs[wdb_si].obs_value_cm

          if (snwd_qc[qcdb_ti, qcdb_si] ne 0) then begin

              PRINT, 'Snow depth value ' + obs_str + ' ' + $
                     'at station "' + site_str + '" ' + $
                     'flagged with value ' + $
                     STRCRA(snwd_qc[qcdb_ti, qcdb_si])

              lun = lunf

          endif else begin

              lun = lunu

          endelse

          PRINTF, lun, $
                  STRCRA(wdb_snow_depth_obs[wdb_si].longitude) + '|' + $
                  STRCRA(wdb_snow_depth_obs[wdb_si].latitude) + '|' + $
                  qc_str + '|' +  $
                  STRTRIM(wdb_snow_depth_obs[wdb_si].date_UTC) + '|' + $
                  STRCRA(wdb_snow_depth_obs[wdb_si].station_id) + '|' + $
                  wdb_snow_depth_obs[wdb_si].station_type + '|' +$
                  wdb_snow_depth_obs[wdb_si].station_source + '|' +$
                  STRCRA(wdb_snow_depth_obs[wdb_si].elevation) + '|' + $
                  STRCRA(wdb_snow_depth_obs[wdb_si].recorded_elevation) $
                  + '|' + $
                  STRCRA(wdb_snow_depth_obs[wdb_si].obs_value_cm) + '|' + $
                  STRCRA(wdb_snow_depth_obs[wdb_si].mdl_value_cm) + '|' + $
                  STRCRA(delta_snwd_cm)

      endif else begin

          PRINT, 'No snow depth QC test for ' + $
                 'snow depth value ' + obs_str + ' ' + $
                 'at station "' + site_str + '"'
   
      endelse

      ;; print, wdb_snow_depth_obs[wdb_si].date_UTC, ' ', $
      ;;        wdb_date_hours[wdb_si], ' ', $
      ;;        qcdb_ti
      ;; if wdb_si gt 10 then BREAK

  endfor

  FREE_LUN, lunf
  FREE_LUN, lunu
  PRINT, 'Results saved to ' + psv_file_flagged + ' and ' + psv_file_unflagged

  PRINT, 'Inventory of QC flags:'
  inv_ind = 0
  for tc = 0, N_ELEMENTS(qc_test_names) - 1 do begin
      if (qc_test_names[tc] eq 'naught') then CONTINUE
      if (qc_test_names[tc] eq 'anomaly') then CONTINUE
      if (qc_test_names[tc] eq 'rate') then CONTINUE
      PRINT, qc_test_names[tc] + ': ' + $
             STRCRA(inventory[inv_ind]) + $
             ' (' + $
             STRCRA(inventory_multiple[inv_ind]) + $
             ' with others, ' + $
             STRCRA(inventory[inv_ind] - inventory_multiple[inv_ind]) + $
             ' alone)'
      inv_ind++
  endfor


  NCDF_CLOSE, id

end
