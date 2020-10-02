; Examine results of quality control on snow depth observations.

; This program looks at snow depth observations on and around a
; specific time and displays those observations and their QC
; information on a map.

FUNCTION GET_WDB_SWE_FOR_TARGET, target_obs_date_YYYYMMDDHH, $
                                 window_start_date_YYYYMMDDHH, $
                                 window_finish_date_YYYYMMDDHH, $
                                 min_lon_, max_lon_, $
                                 min_lat_, max_lat_

; Get hourly SWE observations from the "web_data" database on wdb0.

  swe_report = !NULL


; Check arguments for correct type and valid contents.

  if NOT(ISA(target_obs_date_YYYYMMDDHH, 'STRING')) then begin
      ERR_MSG, 'Target observation date/time argument must be a STRING.'
      RETURN, swe_report
  endif
  if (STRLEN(target_obs_date_YYYYMMDDHH) ne 10) then begin
      ERR_MSG, 'Invalid target observation date/time "' + $
               target_obs_date_YYYYMMDDHH + $
               '" (required form is YYYYMMDDHH, 10 digits).'
      RETURN, swe_report
  endif
  if NOT(STREGEX(target_obs_date_YYYYMMDDHH, '[0-9]{10}', /BOOLEAN)) $
      then begin
      ERR_MSG, 'Invalid target observation date/time "' + $
               target_obs_date_YYYYMMDDHH + $
               '" (required form is YYYYMMDDHH, all numeric).'
      RETURN, swe_report
  endif

  if NOT(ISA(window_start_date_YYYYMMDDHH, 'STRING')) then begin
      ERR_MSG, 'Observation window start date/time argument must be a STRING.'
      RETURN, swe_report
  endif
  if (STRLEN(window_start_date_YYYYMMDDHH) ne 10) then begin
      ERR_MSG, 'Invalid observation window start date/time "' + $
               window_start_date_YYYYMMDDHH + $
               '" (required form is YYYYMMDDHH, 10 digits).'
      RETURN, swe_report
  endif
  if NOT(STREGEX(window_start_date_YYYYMMDDHH, '[0-9]{10}', /BOOLEAN)) $
      then begin
      ERR_MSG, 'Invalid observation window start date/time "' + $
               window_start_date_YYYYMMDDHH + $
               '" (required form is YYYYMMDDHH, all numeric).'
      RETURN, swe_report
  endif

  if NOT(ISA(window_finish_date_YYYYMMDDHH, 'STRING')) then begin
      ERR_MSG, 'Observation window finish date/time argument must be a STRING.'
      RETURN, swe_report
  endif
  if (STRLEN(window_finish_date_YYYYMMDDHH) ne 10) then begin
      ERR_MSG, 'Invalid observation window finish date/time "' + $
               window_finish_date_YYYYMMDDHH + $
               '" (required form is YYYYMMDDHH, 10 digits).'
      RETURN, swe_report
  endif
  if NOT(STREGEX(window_finish_date_YYYYMMDDHH, '[0-9]{10}', /BOOLEAN)) $
      then begin
      ERR_MSG, 'Invalid observation window finish date/time "' + $
               window_finish_date_YYYYMMDDHH + $
               '" (required form is YYYYMMDDHH, all numeric).'
      RETURN, swe_report
  endif

  target_obs_date_Julian = YYYYMMDDHH_TO_JULIAN(target_obs_date_YYYYMMDDHH)
  window_start_date_Julian = YYYYMMDDHH_TO_JULIAN(window_start_date_YYYYMMDDHH)
  window_finish_date_Julian = $
      YYYYMMDDHH_TO_JULIAN(window_finish_date_YYYYMMDDHH)

  if (window_start_date_Julian gt target_obs_date_Julian) then begin
      ERR_MSG, 'Observation window start date/time may not be later than ' + $
               'target observation date/time.'
      RETURN, swe_report
  endif

  if (window_finish_date_Julian lt target_obs_date_Julian) then begin
      ERR_MSG, 'Observation window finish date/time may not be earlier ' + $
               'than target observation date/time.'
      RETURN, swe_report
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
  savFile = 'wdb0_swe' + $
            '_' + target_obs_date_YYYYMMDDHH + $
            '_m' + STRCRA(minus_hrs) + 'h' + $
            '_p' + STRCRA(plus_hrs) + 'h' + $
            '_' + min_lon_file_str + '_to_' + max_lon_file_str + $
            '_' + min_lat_file_str + '_to_' + max_lat_file_str + $
            '.sav'
  savDir = '/net/scratch/nwm_snow_da/wdb0_sav'
  if FILE_TEST(savDir + '/' + savFile) then begin
      RESTORE, savDir + '/' + savFile
      RETURN, swe_report
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
              't3.value_sm_swe ' + $
              'from point.allstation as t1, ' + $
              'point.obs_swe as t2, ' + $
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

  SPAWN, statement, result, EXIT_STATUS = status
  fetch_date_Julian = SYSTIME(/JULIAN, /UTC)

  if (status ne 0) then begin
      ERR_MSG, 'psql statement failed: ' + statement
      RETURN, swe_report
  endif

  num_swe = N_ELEMENTS(result)

  if (result[0] eq '') then begin
      num_swe = 0
      RETURN, swe_report
  endif

; Place results in a structure.

  ndv = -99999.0
  swe_report_ = REPLICATE({station_obj_id: 0L, $
                           station_id: '', $
                           station_name: '', $
                           station_type: '', $
                           station_source: '', $
                           longitude: 0.0d, $
                           latitude: 0.0d, $
                           elevation: 0L, $
                           recorded_elevation: 0L, $
                           date_UTC: '', $
                           obs_value_mm: ndv, $
                           mdl_value_mm: ndv}, $
                          num_swe)

  for sc = 0, num_swe - 1 do begin
      report = STRSPLIT(result[sc], '|', /EXTRACT, /PRESERVE_NULL)
      if (N_ELEMENTS(report) ne 12) then begin
          ERR_MSG, 'Unrecognized structure in SWE reports.'
          num_swe = 0
          RETURN, swe_report
      endif
      swe_report_[sc].station_obj_id = LONG(report[0])
      swe_report_[sc].station_id = report[1]
      swe_report_[sc].station_name = report[2]
      swe_report_[sc].station_type = report[3]
      swe_report_[sc].station_source = report[4]
      swe_report_[sc].longitude = DOUBLE(report[5])
      swe_report_[sc].latitude = DOUBLE(report[6])
      swe_report_[sc].elevation = LONG(report[7])
      swe_report_[sc].recorded_elevation = LONG(report[8])
      swe_report_[sc].date_UTC = report[9]
      swe_report_[sc].obs_value_mm = FLOAT(report[10]) * 1000.0
      if (report[11] ne '') then $
          swe_report_[sc].mdl_value_mm = FLOAT(report[11]) * 1000.0
  endfor

; If any stations delivered multiple reports, choose the one closest
; to the analysis time.

  useFlag = BYTARR(num_swe) & useFlag[*] = 1B

  for sc = 0, num_swe - 1 do begin

      ind = WHERE((swe_report_.station_obj_id eq $
                   swe_report_[sc].station_obj_id) and $
                  (useFlag eq 1B), count)
      if (count eq 0) then begin
          ERR_MSG, 'Programming error.'
          RETURN, swe_report
      endif
      if (count eq 1) then CONTINUE
      useFlag[ind] = 0B         ; set all matches to DO NOT USE
      dateList = swe_report_[ind].date_UTC
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

  ind = WHERE(useFlag eq 1B, num_swe)
  if (num_swe gt 0) then begin
      swe_report_ = swe_report_[ind]
  endif else begin
      ERR_MSG, 'Programming error in unique station search.'
      RETURN, swe_report
  endelse

  swe_report = TEMPORARY(swe_report_)

  if ((fetch_date_Julian - target_obs_date_Julian) gt 60) then $
      SAVE, swe_report, FILENAME = savDir + '/' + savFile

  RETURN, swe_report

end


PRO GET_SWE_DATA_AND_QC, obs_date_Julian, $
                         prev_hours, $
                         station_obj_id, $
                         qcdb_nc_id, $
                         qcdb_ti, $
                         qcdb_si, $
                         sample_num_hours, $
                         swe_sample, $
                         swe_sample_qc
  sample_start_date_Julian = obs_date_Julian - DOUBLE(prev_hours) / 24.0D
  sample_start_YYYYMMDDHH = JULIAN_TO_YYYYMMDDHH(sample_start_date_Julian)
  sample_finish_YYYYMMDDHH = JULIAN_TO_YYYYMMDDHH(obs_date_Julian)
  swe_sample = $
      GET_WDB_OBS_SWE_FOR_RANGE(sample_start_YYYYMMDDHH, $
                                sample_finish_YYYYMMDDHH, $
                                STATION_OBJ_ID = station_obj_id)
  sample_t1_1 = (qcdb_ti - prev_hours) > 0
  sample_num_hours = qcdb_ti - sample_t1_1 + 1
  NCDF_VARGET, qcdb_nc_id, $
               'swe_qc', $
               swe_sample_qc, $
               COUNT = [sample_num_hours, 1], $
               OFFSET = [sample_t1_1, qcdb_si]
  RETURN

end


PRO SHOW_SWE_WRIE_DATA, station_obj_id, $
                        time_str, $
                        obs_str, $
                        mdl_str, $
                        obs_date_Julian, $
                        prev_hrs_wrie, $
                        qcdb_nc_id, $
                        qcdb_ti, $
                        qcdb_si, $
                        ndv, $
                        swe_sample, $
                        swe_sample_qc

  GET_SWE_DATA_AND_QC, obs_date_Julian, $
                       prev_hrs_wrie, $
                       station_obj_id, $
                       qcdb_nc_id, $
                       qcdb_ti, $
                       qcdb_si, $
                       sample_num_hours, $
                       swe_sample, $
                       swe_sample_qc

  site_str = swe_sample.station_id + $
             ' (' + STRCRA(station_obj_id) + ')' + $
             ' (' + STRCRA(swe_sample.station_type) + ')'

  PRINT, '    ' + $
         site_str + ' - ' + $
         time_str + ' - ' + $
         ' obs ' + obs_str + ' mm, ' + $
         ' mdl ' + mdl_str + ' mm'

; Basic logic check - the last SWE obs we just fetched should be the
; one we are interested in.
  if (swe_sample.obs_value_mm[prev_hrs_wrie] eq ndv) then STOP

; Pad QC data with 9999 for sample data falling outside the database.
  if (sample_num_hours lt (prev_hrs_wrie + 1)) then begin
      swe_sample_qc = $
          [REPLICATE(9999, prev_hrs_wrie - sample_num_hours + 1), $
           swe_sample_qc]
  endif
  if (swe_sample_qc[prev_hrs_wrie] eq 0) then STOP ; basic logic check

; Get indices of SWE obs that meet two conditions:
;   - not a no data value
;   - has no QC flag set, or is from a time outside the database
  ok_ind = WHERE((swe_sample.obs_value_mm[0:prev_hrs_wrie-1] ne ndv) and $
                 ((swe_sample_qc[0:prev_hrs_wrie-1] eq 0) or $
                  (swe_sample_qc[0:prev_hrs_wrie-1] eq 9999)), ok_count)
  if (ok_count eq 0) then STOP

; Identify the reference observation that was used to set the flag.
  ref_swe = MIN(swe_sample.obs_value_mm[ok_ind], ind)
  ref_swe_ind = ok_ind[ind]
  ref_date_Julian = obs_date_Julian - $
                    DOUBLE(prev_hrs_wrie - ref_swe_ind) / 24.0D
  obs_date_str = JULIAN_TO_YYYYMMDDHH(obs_date_Julian)
  ref_date_str = JULIAN_TO_YYYYMMDDHH(ref_date_Julian)

  swe_sample_str = STRCRA(swe_sample.obs_value_mm)
  ind = WHERE(swe_sample.obs_value_mm eq ndv, count)
  if (count gt 0) then swe_sample_str[ind] = '-'
  ind = WHERE((swe_sample_qc gt 0) and $
              (swe_sample_qc ne 9999), count)
  if (count gt 0) then $
      swe_sample_str[ind] = swe_sample_str[ind] + $
                            '(qc=' + STRCRA(swe_sample_qc[ind]) + ')'

  PRINT, '    SWE change ' + $
         STRCRA(swe_sample.obs_value_mm[prev_hrs_wrie] - ref_swe) + $
         ' mm ' + $
         '(' + STRCRA(ref_swe) + ' to ' + $
         STRCRA(swe_sample.obs_value_mm[prev_hrs_wrie]) + ', ' + $
         ref_date_str + ' to ' + obs_date_str + ')'
  ;if (sample_num_hours gt prev_hrs_wrie) then begin
  ;; PRINT, swe_sample_str
  ;; PRINT, obs_str, ' (', $
  ;;        swe_sample.station_type + ')'
  ;endif

; Generate a URL for a NSA time series.
  web_start_date_Julian = obs_date_Julian - $
                          DOUBLE(prev_hrs_wrie) / 24.0D - $
                          1.5D
  web_start_date_YYYYMMDDHH = JULIAN_TO_YYYYMMDDHH(web_start_date_Julian)
  web_finish_date_Julian = obs_date_Julian + 1.5D
  web_finish_date_YYYYMMDDHH = JULIAN_TO_YYYYMMDDHH(web_finish_date_Julian)
  url = 'https://www.nohrsc.noaa.gov/interactive/html/graph.html' + $
        '?station=' + swe_sample.station_id + $
        '&w=800&h=600&o=a&uc=0' + $
        '&by=' + STRMID(web_start_date_YYYYMMDDHH, 0, 4) + $
        '&bm=' + STRMID(web_start_date_YYYYMMDDHH, 4, 2) + $
        '&bd=' + STRMID(web_start_date_YYYYMMDDHH, 6, 2) + $
        '&bh=' + STRMID(web_start_date_YYYYMMDDHH, 8, 2) + $
        '&ey=' + STRMID(web_finish_date_YYYYMMDDHH, 0, 4) + $
        '&em=' + STRMID(web_finish_date_YYYYMMDDHH, 4, 2) + $
        '&ed=' + STRMID(web_finish_date_YYYYMMDDHH, 6, 2) + $
        '&eh=' + STRMID(web_finish_date_YYYYMMDDHH, 8, 2) + $
        '&data=0&units=1&region=us'
  PRINT, '    time series URL:'
  PRINT, '    ' + url

; Generate a URL for a map of SWE observations.
  obs_date_YYYYMMDDHH = JULIAN_TO_YYYYMMDDHH(obs_date_Julian)
  width = 1200
  height = 675
  dy = 1.0D
  dx = dy * DOUBLE(width) / DOUBLE(height)

  url = 'https://www.nohrsc.noaa.gov/interactive/html/map.html' + $
        '?ql=station&zoom=&zoom7.x=16&zoom7.y=10' + $
        '&loc=Latitude%2CLongitude%3B+City%2CST%3B+or+Station+ID' + $
        '&var=swe_obs_5_h' + $
        '&dy=' + STRMID(obs_date_YYYYMMDDHH, 0, 4) + $
        '&dm=' + STRMID(obs_date_YYYYMMDDHH, 4, 2) + $
        '&dd=' + STRMID(obs_date_YYYYMMDDHH, 6, 2) + $
        '&dh=' + STRMID(obs_date_YYYYMMDDHH, 8, 2) + $
        '&snap=1&o9=1&o12=1&lbl=l&o13=1&mode=pan&extents=us' + $
        '&min_x=' + STRCRA(swe_sample.longitude - 0.5D * dx) + $
        '&min_y=' + STRCRA(swe_sample.latitude - 0.5D * dy) + $
        '&max_x=' + STRCRA(swe_sample.longitude + 0.5D * dx) + $
        '&max_y=' + STRCRA(swe_sample.latitude + 0.5D * dy) + $
        '&coord_x=' + STRCRA(swe_sample.longitude) + $
        '&coord_y=' + STRCRA(swe_sample.latitude) + $
        '&zbox_n=&zbox_s=&zbox_e=&zbox_w=&metric=1&bgvar=dem' + $
        ;; '&shdvar=shading' + $
        '&width=' + STRCRA(width) + $
        '&height=' + STRCRA(height) + $
        '&nw=' + STRCRA(width) + $
        '&nh=' + STRCRA(height) + $
        '&h_o=0&font=0&js=1&uc=0'
  PRINT, '    observation map URL:'
  PRINT, '    ' + url

  RETURN

end


PRO SHOW_SWE_STREAK_DATA, station_obj_id, $
                          time_str, $
                          obs_str, $
                          mdl_str, $
                          obs_date_Julian, $
                          prev_hrs_streak, $
                          qcdb_nc_id, $
                          qcdb_ti, $
                          qcdb_si, $
                          ndv, $
                          swe_sample, $
                          swe_sample_qc

  GET_SWE_DATA_AND_QC, obs_date_Julian, $
                       prev_hrs_streak, $
                       station_obj_id, $
                       qcdb_nc_id, $
                       qcdb_ti, $
                       qcdb_si, $
                       sample_num_hours, $
                       swe_sample, $
                       swe_sample_qc

  
  site_str = swe_sample.station_id + $
             ' (' + STRCRA(station_obj_id) + ')' + $
             ' (' + STRCRA(swe_sample.station_type) + ')'
  
  PRINT, '    ' + $
         site_str + ' - ' + $
         time_str + ' - ' + $
         ' obs ' + obs_str + ' mm' + $
         ' (mdl ' + mdl_str + ' mm)'

; Basic logic check - the last SWE obs we just fetched should be the
; one we are interested in.
  if (swe_sample.obs_value_mm[prev_hrs_streak] eq ndv) then STOP

; Pad QC data with 9999 for sample data falling outside the database.
  if (sample_num_hours lt (prev_hrs_streak + 1)) then begin
      swe_sample_qc = $
          [REPLICATE(9999, prev_hrs_streak - sample_num_hours + 1), $
           swe_sample_qc]
  endif
  if (swe_sample_qc[prev_hrs_streak] eq 0) then STOP ; basic logic check

  ok_ind = WHERE((swe_sample.obs_value_mm[0:prev_hrs_streak-1] ne ndv) and $
                 ((swe_sample_qc[0:prev_hrs_streak-1] eq 0) or $
                  (swe_sample_qc[0:prev_hrs_streak-1] eq 9999)), ok_count)
  if (ok_count eq 0) then STOP

; Identify the minimum and maximum streak dates.
  min_streak_date = obs_date_Julian - $
                    DOUBLE(prev_hrs_streak - ok_ind[0]) / 24.0D
  max_streak_date = obs_date_Julian - $
                    DOUBLE(prev_hrs_streak - ok_ind[ok_count - 1]) / 24.0D
  ;; min_swe = MIN(swe_sample.obs_value_mm[ok_ind], ind)
  ;; min_swe_ind = ok_ind[ind]
  ;; min_date_Julian = obs_date_Julian - $
  ;;                   DOUBLE(prev_hours_swe - min_swe_ind) / 24.0D
  ;; max_swe = MAX(swe_sample.obs_value_mm[ok_ind], ind)
  ;; max_swe_ind = ok_ind[ind]
  ;; max_date_Julian = obs_date_Julian - $
  ;;                   DOUBLE(prev_hours_swe - max_swe_ind) / 24.0D

  min_streak_date_str = JULIAN_TO_YYYYMMDDHH(min_streak_date)
  max_streak_date_str = JULIAN_TO_YYYYMMDDHH(max_streak_date)
  obs_date_YYYYMMDDHH = JULIAN_TO_YYYYMMDDHH(obs_date_Julian)

  swe_sample_str = STRCRA(swe_sample.obs_value_mm)
  ind = WHERE(swe_sample.obs_value_mm eq ndv, count)
  if (count gt 0) then swe_sample_str[ind] = '-'
  ind = WHERE((swe_sample_qc gt 0) and $
              (swe_sample_qc ne 9999), count)
  if (count gt 0) then $
      swe_sample_str[ind] = swe_sample_str[ind] + $
                            '(qc=' + STRCRA(swe_sample_qc[ind]) + ')'
  streak_length = obs_date_Julian - min_streak_date
  
  PRINT, '    SWE streak ' + $
         'from ' + STRCRA(min_streak_date_str) + $
         ' (obs ' + STRCRA(swe_sample.obs_value_mm[ok_ind[0]]) + ') ' + $
         'to ' + STRCRA(max_streak_date_str) + $
         ' (obs ' + STRCRA(swe_sample.obs_value_mm[ok_ind[ok_count - 1]]) + $
         ')' + $
         ', then ' + STRCRA(swe_sample.obs_value_mm[prev_hrs_streak]) + $
         ' at ' + STRCRA(obs_date_YYYYMMDDHH) + $
         ' (' + STRCRA(ROUND(streak_length * 24.0D) + 1) + ' hours)'

  web_start_date_Julian = obs_date_Julian - $
                          DOUBLE(prev_hrs_streak) / 24.0D - $
                          1.5D
  web_start_date_YYYYMMDDHH = JULIAN_TO_YYYYMMDDHH(web_start_date_Julian)
  web_finish_date_Julian = obs_date_Julian + 1.5D
  web_finish_date_YYYYMMDDHH = JULIAN_TO_YYYYMMDDHH(web_finish_date_Julian)
  url = 'https://www.nohrsc.noaa.gov/interactive/html/graph.html' + $
        '?station=' + swe_sample.station_id + $
        '&w=800&h=600&o=a&uc=0' + $
        '&by=' + STRMID(web_start_date_YYYYMMDDHH, 0, 4) + $
        '&bm=' + STRMID(web_start_date_YYYYMMDDHH, 4, 2) + $
        '&bd=' + STRMID(web_start_date_YYYYMMDDHH, 6, 2) + $
        '&bh=' + STRMID(web_start_date_YYYYMMDDHH, 8, 2) + $
        '&ey=' + STRMID(web_finish_date_YYYYMMDDHH, 0, 4) + $
        '&em=' + STRMID(web_finish_date_YYYYMMDDHH, 4, 2) + $
        '&ed=' + STRMID(web_finish_date_YYYYMMDDHH, 6, 2) + $
        '&eh=' + STRMID(web_finish_date_YYYYMMDDHH, 8, 2) + $
        '&data=0&units=1&region=us'
  PRINT, '    time series URL:'
  PRINT, '    ' + url

  width = 1200
  height = 675
  dy = 1.0D
  dx = dy * DOUBLE(width) / DOUBLE(height)

  url = 'https://www.nohrsc.noaa.gov/interactive/html/map.html' + $
        '?ql=station&zoom=&zoom7.x=16&zoom7.y=10' + $
        '&loc=Latitude%2CLongitude%3B+City%2CST%3B+or+Station+ID' + $
        '&var=swe_obs_5_h' + $
        '&dy=' + STRMID(obs_date_YYYYMMDDHH, 0, 4) + $
        '&dm=' + STRMID(obs_date_YYYYMMDDHH, 4, 2) + $
        '&dd=' + STRMID(obs_date_YYYYMMDDHH, 6, 2) + $
        '&dh=' + STRMID(obs_date_YYYYMMDDHH, 8, 2) + $
        '&snap=1&o9=1&o12=1&lbl=l&o13=1&mode=pan&extents=us' + $
        '&min_x=' + STRCRA(swe_sample.longitude - 0.5D * dx) + $
        '&min_y=' + STRCRA(swe_sample.latitude - 0.5D * dy) + $
        '&max_x=' + STRCRA(swe_sample.longitude + 0.5D * dx) + $
        '&max_y=' + STRCRA(swe_sample.latitude + 0.5D * dy) + $
        '&coord_x=' + STRCRA(swe_sample.longitude) + $
        '&coord_y=' + STRCRA(swe_sample.latitude) + $
        '&zbox_n=&zbox_s=&zbox_e=&zbox_w=&metric=1&bgvar=dem' + $
        ;; '&shdvar=shading' + $
        '&width=' + STRCRA(width) + $
        '&height=' + STRCRA(height) + $
        '&nw=' + STRCRA(width) + $
        '&nh=' + STRCRA(height) + $
        '&h_o=0&font=0&js=1&uc=0'
  PRINT, '    observation map URL:'
  PRINT, '    ' + url
  ;PRINT, swe_sample_str

  RETURN

end



PRO GET_SNWD_TAIR_DATA, obs_date_Julian, $
                        prev_hours_tair, $
                        station_obj_id, $
                        tair_sample

  sample_start_date_Julian = obs_date_Julian - DOUBLE(prev_hours_tair) / 24.0D
  sample_start_YYYYMMDDHH = JULIAN_TO_YYYYMMDDHH(sample_start_date_Julian)
  sample_finish_YYYYMMDDHH = JULIAN_TO_YYYYMMDDHH(obs_date_Julian)
  snwd_sample = $
      GET_WDB_OBS_AIR_TEMP_FOR_RANGE(sample_start_YYYYMMDDHH, $
                                     sample_finish_YYYYMMDDHH, $
                                     STATION_OBJ_ID = station_obj_id)
  RETURN

end


PRO SHOW_SNWD_TAIR_DATA, site_str, $
                         time_str, $
                         obs_str, $
                         mdl_str, $
                         obs_date_Julian, $
                         prev_hours_tair, $
                         station_obj_id, $
                         qcdb_nc_id, $
                         qcdb_ti, $
                         qcdb_si, $
                         ndv, $
                         sample_num_hours, $
                         snwd_sample, $
                         snwd_sample_qc

; Use GET_SNWD_WRIE_DATA to fetch snow depth data and corresponding QC
; data for comparison with air temperature data.
  GET_SNWD_WRIE_DATA, obs_date_Julian, $
                      prev_hours_tair, $
                      station_obj_id, $
                      qcdb_nc_id, $
                      qcdb_ti, $
                      qcdb_si, $
                      sample_num_hours, $
                      snwd_sample, $
                      snwd_sample_qc
  
  PRINT, '  ' + $
         site_str + ' - ' + $
         time_str + ' - ' + $
         ' obs ' + obs_str + ', ' + $
         ' mdl ' + mdl_str
  PRINT, '  qcdb_si = ' + STRCRA(qcdb_si) + $
         ', qcdb_ti = ' + STRCRA(qcdb_ti)

  sample_start_date_Julian = obs_date_Julian - DOUBLE(prev_hours_tair) / 24.0D
  sample_start_YYYYMMDDHH = JULIAN_TO_YYYYMMDDHH(sample_start_date_Julian)
  sample_finish_YYYYMMDDHH = JULIAN_TO_YYYYMMDDHH(obs_date_Julian)
  tair_sample = GET_WDB_OBS_AIR_TEMP_FOR_RANGE(sample_start_YYYYMMDDHH, $
                                               sample_finish_YYYYMMDDHH, $
                                               STATION_OBJ_ID = station_obj_id)

  tair_sample_str = STRCRA(tair_sample.obs_value_deg_c)
  ind = WHERE(tair_sample.obs_value_deg_c eq ndv, count)
  if (count gt 0) then tair_sample_str[ind] = '-'
  snwd_sample_str = STRCRA(snwd_sample.obs_value_cm)
  ind = WHERE(snwd_sample.obs_value_cm eq ndv, count)
  if (count gt 0) then snwd_sample_str[ind] = '-'
  ind = WHERE(snwd_sample_qc ne 0, count)
  if (count gt 0) then $
      snwd_sample_str[ind] = snwd_sample_str[ind] + $
                             '(qc=' + STRCRA(snwd_sample_qc[ind]) + ')'
  if (sample_num_hours gt prev_hours_tair) then begin
      for ti = 0, sample_num_hours - 1 do $
          PRINT, snwd_sample_str[ti] + '  ' + tair_sample_str[ti]
      ;; PRINT, snwd_sample_str
      ;; PRINT, tair_sample_str
      PRINT, '---'
      PRINT, obs_str, ' (', $
             snwd_sample.station_type + ')'
      move = GET_KBRD(1)
  endif

  RETURN

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
  start_date_YYYYMMDDHH = '2020010100'
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
  step_size_days = 0.0
  ;; full_run = 1

  if ((num_hrs_pad_prev + num_hrs_pad_post) ge cluster_gap_hours) then begin
      ERR_MSG, 'Invalid padding values.'
      STOP
  endif
  ;; sav_file = 'eval_swe_qc_' + $
  ;;            start_date_YYYYMMDDHH + '_to_' + finish_date_YYYYMMDDHH + '.sav'
  ;; if NOT(full_run) then begin
  ;;     RESTORE, sav_file
  ;;     full_run = 0
  ;;     GOTO, SKIP
  ;; endif

; Identify the QC database.
  qcdb_dir = '/net/scratch/fall/m1_dev'
  qcdb_date_range = '2019100100_to_2020093023'
  qcdb_file = 'station_qc_db_' + qcdb_date_range + '.nc.0707'
  qcdb_path = qcdb_dir + '/' + qcdb_file
  if NOT(FILE_TEST(qcdb_path)) then STOP

; Odds and ends.
  ndv = -99999.0
  generate_psv_files = 0
  ;; abs_diff_thresh_cm = [16.0, 122.0]

  if generate_psv_files then begin
      login_info = GET_LOGIN_INFO()
      psv_dir = '/net/scratch/' + login_info.user_name
      if NOT(FILE_TEST(psv_dir, /DIRECTORY)) then STOP
  endif

; Open the QC database.
  qcdb_nc_id = NCDF_OPEN(qcdb_path)

; Check QC database time range against evaluation time period.
  NCDF_ATTGET, qcdb_nc_id, $
               'last_datetime_updated', $
               last_datetime_updated, $
               /GLOBAL
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
  NCDF_ATTGET, qcdb_nc_id, $
               'swe_qc', $
               'qc_test_names', $
               qc_test_names
  qc_test_names = STRING(qc_test_names)
  NCDF_ATTGET, qcdb_nc_id, $
               'swe_qc', $
               'qc_test_bits', $
               qc_test_bits
  if (N_ELEMENTS(qc_test_names) ne N_ELEMENTS(qc_test_bits)) then STOP

; Produce false alarm debugging information for a specific test.
  eval_test_name = 'streak'
  ;; eval_test_name = 'world_record_increase_exceedance'
  ;; eval_test_name = 'temperature_consistency'
  ;; eval_test_name = 'snowfall_consistency'

  eval_test_ind = !NULL
  ti = 0
  for tc = 0, N_ELEMENTS(qc_test_names) - 1 do begin
      if (qc_test_names[tc] eq 'naught') then CONTINUE
      if (qc_test_names[tc] eq 'anomaly') then CONTINUE
      if (qc_test_names[tc] eq 'rate') then CONTINUE
      if (qc_test_names[tc] eq eval_test_name) then eval_test_ind = ti
      ti++
  endfor
  if NOT(ISA(eval_test_ind)) then STOP

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
  NCDF_VARGET, qcdb_nc_id, 'station_obj_identifier', qcdb_station_obj_id
  NCDF_VARGET, qcdb_nc_id, 'station_id', qcdb_station_id
  NCDF_VARGET, qcdb_nc_id, 'station_longitude', qcdb_station_longitude
  NCDF_VARGET, qcdb_nc_id, 'station_latitude', qcdb_station_latitude
  qcdb_num_stations = N_ELEMENTS(qcdb_station_obj_id)

; Get time variables from the QC database.
  NCDF_VARGET, qcdb_nc_id, 'time', qcdb_time
  NCDF_ATTGET, qcdb_nc_id, 'time', 'units', qcdb_time_units
  qcdb_time_units = STRING(qcdb_time_units)
  if (qcdb_time_units ne 'hours since 1970-01-01 00:00:00 UTC') then STOP
  qcdb_time_base_Julian = JULDAY(1, 1, 1970, 0, 0, 0)
  qcdb_time_start_hours = qcdb_time[0]
  qcdb_time_start_Julian = qcdb_time_base_Julian + $
                           qcdb_time_start_hours / 24.0D

; Initialize false alarm ratios.
  FAR = []
  pFAR = []
  solo_freq = []
  num_flagged_obs = []
  solo_FAR = []
  solo_pFAR = []
  num_solo_flagged_obs = []
  cluster_mean_date_Julian = []

  eval_test_num_flagged = 0L
  eval_test_hit = 0L ; a
  eval_test_false = 0L ; b
  eval_test_unsure = 0L         ; a or b
  eval_test_obj_id = []
  eval_test_date_Julian = []
  eval_test_val = []
  
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
      lfa_count = 0L ; false alarms
      pfa_count = 0L ; possible false alarms

      date_total = 0.0D

      for cc = 0, cluster_size - 1 do begin

          date_total = date_total + date_Julian
          eval_target_YYYYMMDDHH = JULIAN_TO_YYYYMMDDHH(date_Julian)

          PRINT, '  Get QC data for ' + eval_target_YYYYMMDDHH

          if generate_psv_files then begin
              psv_file_flagged = psv_file_dir + '/' + $
                                 'SWE_QC_' + eval_target_YYYYMMDDHH + $
                                 '_flagged.txt'
              psv_file_unflagged = psv_file_dir + '/' + $
                                   'SWE_QC_' + eval_target_YYYYMMDDHH + $
                                   '_unflagged.txt'
              OPENW, lunf, psv_file_flagged, /GET_LUN
              OPENW, lunu, psv_file_unflagged, /GET_LUN

              PRINTF, lunf, $
                      'longitude|latitude|' + qc_test_names_str + $
                      '|date|station_id|' + $
                      'station_type|station_source|' + $
                      'elevation|rec_elevation|' + $
                      'obs_swe_mm|mdl_swe_mm|' + $
                      'delta_swe_mm'
              PRINTF, lunu, $
                      'longitude|latitude|' + qc_test_names_str + $
                      '|date|station_id|' + $
                      'station_type|station_source|' + $
                      'elevation|rec_elevation|' + $
                      'obs_swe_mm|mdl_swe_mm|' + $
                      'delta_swe_mm'
          endif

;         Calculate start/finish times for the narrow period of hours
;         that can be roughly attributed to eval_target_YYYYMMDDHH.
          eval_target_Julian = YYYYMMDDHH_TO_JULIAN(eval_target_YYYYMMDDHH)
          eval_window_start_Julian = $
              eval_target_Julian - DOUBLE(num_hrs_pad_prev) / 24.0D
          eval_window_start_YYYYMMDDHH = $
              JULIAN_TO_YYYYMMDDHH(eval_window_start_Julian)
          eval_window_finish_Julian = $
              eval_target_Julian + DOUBLE(num_hrs_pad_prev) / 24.0D
          eval_window_finish_YYYYMMDDHH = $
              JULIAN_TO_YYYYMMDDHH(eval_window_finish_Julian)

;         Get SWE observations.
          wdb_swe_obs = $
              GET_WDB_SWE_FOR_TARGET(eval_target_YYYYMMDDHH, $
                                     eval_window_start_YYYYMMDDHH, $
                                     eval_window_finish_YYYYMMDDHH, $
                                     min_lon, max_lon, $
                                     min_lat, max_lat)

          PRINT, '  Found ' + STRCRA(N_ELEMENTS(wdb_swe_obs)) + $
                 ' SWE obs.'

;         Determine QC database time indices for these observations.
          eval_window_start_qcdb = $
              ROUND((eval_window_start_Julian - qcdb_time_base_Julian) * 24)
          qcdb_t1_1 = WHERE(qcdb_time eq eval_window_start_qcdb, count)
          if (count ne 1) then STOP
          qcdb_t1_1 = qcdb_t1_1[0]
          ;Probably better:
          ;qcdb_t1_1 = ROUND((eval_window_start_Julian - qcdb_time_start_Julian) * 24)

          eval_window_finish_qcdb = $
              ROUND((eval_window_finish_Julian - qcdb_time_base_Julian) * 24)
          qcdb_t1_2 = WHERE(qcdb_time eq eval_window_finish_qcdb, count)
          if (count ne 1) then STOP
          qcdb_t1_2 = qcdb_t1_2[0]
          ;Probably better:
          ;qcdb_t1_2 = ROUND((eval_window_finish_Julian - qcdb_time_start_Julian) * 24)

          if ((qcdb_t1_2 - qcdb_t1_1) ne $
              (num_hrs_pad_prev + num_hrs_pad_post)) then STOP

;         Get QC data for SWE observations.
          NCDF_VARGET, qcdb_nc_id, 'swe_qc', swe_qc, $
                       COUNT = [num_hrs_pad_prev + num_hrs_pad_post + 1, $
                                qcdb_num_stations], $
                       OFFSET = [qcdb_t1_1, 0]
          NCDF_VARGET, qcdb_nc_id, 'swe_qc_checked', swe_qc_checked, $
                       COUNT = [num_hrs_pad_prev + num_hrs_pad_post + 1, $
                                qcdb_num_stations], $
                       OFFSET = [qcdb_t1_1, 0]

;         Determine the QC data time index for each snow depth
;         observation.
          obs_date = wdb_swe_obs.date_UTC
          wdb_obs_date_Julian = $
              JULDAY(FIX(STRMID(obs_date, 5, 2)), $
                     FIX(STRMID(obs_date, 8, 2)), $
                     FIX(STRMID(obs_date, 0, 4)), $
                     FIX(STRMID(obs_date, 11, 2)), $
                     FIX(STRMID(obs_date, 14, 2)), $
                     FIX(STRMID(obs_date, 17, 2)))
          wdb_date_hours = $
              ROUND((wdb_obs_date_Julian - qcdb_time_base_Julian) * 24.0)
          obs_date = !NULL

;+
;         Loop over observations for the current date/time.
;-
          for wdb_si = 0, N_ELEMENTS(wdb_swe_obs) - 1 do begin

              obs_date_Julian = wdb_obs_date_Julian[wdb_si]

              if ((wdb_swe_obs[wdb_si].mdl_value_mm eq ndv) or $
                  (wdb_swe_obs[wdb_si].obs_value_mm eq ndv)) then begin
                  ERR_MSG, 'WARNING: STATION "' + $
                           wdb_swe_obs[wdb_si].station_id + $
                           '" has missing values; skipping.'
                  CONTINUE
              endif
                           
;             Locate the position of this station in the QC database.
              qcdb_si = WHERE(qcdb_station_obj_id eq $
                              wdb_swe_obs[wdb_si].station_obj_id, $
                              count)
              if (count eq 0) then begin
                  ERR_MSG, 'WARNING: station "' + $
                           wdb_swe_obs[wdb_si].station_id + $
                           '" not found in QC database; skipping.'
                  CONTINUE
              endif
              if (count ne 1) then STOP
              qcdb_si = qcdb_si[0]

;             Get the time index of this observation, relative to the
;             narrow eval_window.
              eval_window_ti = wdb_date_hours[wdb_si] - eval_window_start_qcdb

;             Get the time index of this observation, relative to the
;             database time dimension.
              qcdb_ti = qcdb_t1_1 + eval_window_ti

;             Extract station metadata.
              station_obj_id = wdb_swe_obs[wdb_si].station_obj_id
              station_id = wdb_swe_obs[wdb_si].station_id
              station_lon = wdb_swe_obs[wdb_si].longitude
              station_lat = wdb_swe_obs[wdb_si].latitude

;             Generate a few strings for convenience.
              obs_str = STRCRA(wdb_swe_obs[wdb_si].obs_value_mm)
              obs_str_in = $
                  STRCRA(wdb_swe_obs[wdb_si].obs_value_mm / 25.4)
              mdl_str = STRCRA(wdb_swe_obs[wdb_si].mdl_value_mm)
              mdl_str_in = $
                  STRCRA(wdb_swe_obs[wdb_si].mdl_value_mm / 25.4)
              site_str = STRCRA(wdb_swe_obs[wdb_si].station_id)
              time_str = wdb_swe_obs[wdb_si].date_UTC


              if (swe_qc_checked[eval_window_ti, qcdb_si] eq 0) then CONTINUE

;             Decode QC value.
              qc_str = ''
              qc_val = swe_qc[eval_window_ti, qcdb_si]
              qc_val_str = STRING(qc_val, FORMAT = '(B)')

;             Replace whitespace padding in qc_val_str with zero padding
;             for higher (unused) bits.
              STR_REPLACE, qc_val_str, ' ', '0'

;             Initialize inventories for flags.
              if NOT(ISA(inventory)) then begin
;                 Count QC tests and initialize test inventories.
                  inv_ind = 0
                  num_qc_tests = 0
                  for tc = 0, N_ELEMENTS(qc_test_names) - 1 do begin
                      if (qc_test_names[tc] eq 'naught') then CONTINUE
                      if (qc_test_names[tc] eq 'anomaly') then CONTINUE
                      if (qc_test_names[tc] eq 'rate') then CONTINUE
                      if (qc_test_names[tc] eq 'temperature_consistency') $
                      then CONTINUE
                      if (qc_test_names[tc] eq 'snowfall_consistency') $
                      then CONTINUE
                      if (qc_test_names[tc] eq $
                          'spatial_temperature_consistency') then CONTINUE
                      num_qc_tests++
                  endfor
                  inventory = LONARR(num_qc_tests)
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
                  if (qc_test_names[tc] eq 'temperature_consistency') $
                  then CONTINUE
                  if (qc_test_names[tc] eq 'snowfall_consistency') $
                  then CONTINUE
                  if (qc_test_names[tc] eq $
                      'spatial_temperature_consistency') then CONTINUE
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
                  
;             Identify "solo" flags. If any test is the only one to flag a
;             report, note this using solo_ti and solo_inventory.
              solo_ti = -1
              for tc = 0, num_qc_tests - 1 do begin
                  if ((this_inventory[tc] eq 1) and $
                      (TOTAL(this_inventory) eq 1)) then begin
                      solo_ti = tc
                      solo_inventory[tc] = solo_inventory[tc] + 1
                  endif
              endfor

;             Increment the full inventory.
              inventory = inventory + this_inventory

              if (swe_qc[eval_window_ti, qcdb_si] eq 0) then CONTINUE

;             This observation has been flagged by at least one test.

;             If the observation was flagged by the test identified by
;             eval_test_name and eval_test_ind, provide contextual data and
;             allow the user to decide if the flag was a false alarm.

              if ((this_inventory[eval_test_ind] eq 1) and $
                  (eval_test_name eq 'world_record_increase_exceedance')) $
              then begin

                  PRINT, '  "' + eval_test_name + '" test flagged a report'

                  prev_hrs_wrie = 24
                  SHOW_SWE_WRIE_DATA, station_obj_id, $
                                      time_str, $
                                      obs_str, $
                                      mdl_str, $
                                      obs_date_Julian, $
                                      prev_hrs_wrie, $
                                      qcdb_nc_id, $
                                      qcdb_ti, $
                                      qcdb_si, $
                                      ndv, $
                                      swe_sample, $
                                      swe_sample_qc

                  PRINT, '  pressakey' & move = GET_KBRD(1)

              endif

              if ((this_inventory[eval_test_ind] eq 1) and $
                  (eval_test_name eq 'streak')) then begin
                  PRINT, '  Test "' + eval_test_name + '" flagged a report:'
                  PRINT, '    ----'

                  prev_hrs_streak = 360
                  SHOW_SWE_STREAK_DATA, station_obj_id, $
                                        time_str, $
                                        obs_str, $
                                        mdl_str, $
                                        obs_date_Julian, $
                                        prev_hrs_streak, $
                                        qcdb_nc_id, $
                                        qcdb_ti, $
                                        qcdb_si, $
                                        ndv, $
                                        swe_sample, $
                                        swe_sample_qc

                  PRINT, '    ----'
                  flag_obs_val = swe_sample.obs_value_mm[prev_hrs_streak]
                  PRINT, '    Reported SWE = ' + $
                         STRCRA(flag_obs_val) + $
                         ' mm at ' + $
                         swe_sample.station_id + $
                         ', ' + $
                         time_str + $
                         ' flagged by "' + eval_test_name + '" test.'
                  ind = WHERE(eval_test_obj_id eq station_obj_id, count)
                  if (count eq 0) then begin
                      eval_test_obj_id = [eval_test_obj_id, station_obj_id]
                      eval_test_date_Julian = [eval_test_date_Julian, $
                                               obs_date_Julian]
                      eval_test_val = [eval_test_val, flag_obs_val]
                  endif else begin
                      if (count gt 1) then STOP
                      ind = ind[0]
                      PRINT, '    (NOTE: station has been flagged before ' + $
                             'in this evaluation:)'
                      days_ago = obs_date_Julian - eval_test_date_Julian[ind]
                      hrs_ago = ROUND(days_ago * 24.0D)
                      date = JULIAN_TO_GISRS_DATE(eval_test_date_Julian[ind])
                      PRINT, '    (SWE = ' + STRCRA(eval_test_val[ind]) + $
                             ' mm, ' + $
                             date + '; ' + STRCRA(hrs_ago) + ' hours earlier)'
;                     If the same observed value at this site was
;                     flagged less than prev_hrs_streak ago, do not
;                     re-evaluate it.
                      if ((hrs_ago lt prev_hrs_streak) and $
                          (flag_obs_val eq eval_test_val[ind])) then begin
                          PRINT, '    (SKIPPING)'
                          CONTINUE ; do not update!
                      endif
                      eval_test_date_Julian[ind] = obs_date_Julian
                      eval_test_val[ind] = flag_obs_val
                  endelse
                  PRINT, '    Is this a good observation (i.e., false ' + $
                         'alarm) or a bad observation (i.e., hit)?'
                  choice = '*'
                  while ((choice ne 'g') and $
                         (choice ne 'b') and $
                         (choice ne 'n') and $
                         (choice ne 's')) do begin
                      PRINT, '    (g)ood obs, (b)ad obs, ' + $
                             '(n)ot sure, (s)kip: '
                      choice = STRLOWCASE(GET_KBRD(1))
                  endwhile
                  if (choice ne 's') then begin
                      eval_test_num_flagged++
                      case choice of
                          'g': eval_test_false++
                          'b': eval_test_hit++
                          'n': eval_test_unsure++
                      endcase
                  endif
                  if (eval_test_num_flagged gt 0) then begin
                      eval_test_likely_far = $
                          FLOAT(eval_test_false) / $
                          FLOAT(eval_test_num_flagged)
                      eval_test_possible_far = $
                          FLOAT(eval_test_false + eval_test_unsure) / $
                          FLOAT(eval_test_num_flagged)
                                ; Show FAR result so far.
                      ;; PRINT, '# evaluated = ' + $
                      ;;        STRCRA(eval_test_num_flagged) + $
                      ;;        ', # likely FA = ' + $
                      ;;        STRCRA(eval_test_false) + $
                      ;;        ', # possible FA = ' + $
                      ;;        STRCRA(eval_test_unsure)
                      PRINT, '  Likely FAR ' + $
                             STRCRA(eval_test_false) + '/' + $
                             STRCRA(eval_test_num_flagged) + $
                             ' = ' + $
                             STRCRA(eval_test_likely_far) + $
                             ', possible FAR ' + $
                             STRCRA(eval_test_false + eval_test_unsure) + $
                             '/' + $
                             STRCRA(eval_test_num_flagged) + $
                             ' = ' + $
                             STRCRA(eval_test_possible_far) + $
                             ', # evaluated = ' + $
                             STRCRA(eval_test_num_flagged)
                  endif

              endif

              CONTINUE

                      case 1 of
                          (abs_diff[wdb_si] ge 0.0) and $
                              (abs_diff[wdb_si] lt abs_diff_thresh_cm[0]): $
                              begin
                              ; likely false alarm
                              inventory_fa = inventory_fa + this_inventory
                              if (solo_ti ne -1) then $
                                  solo_inventory_fa[solo_ti] = $
                                  solo_inventory_fa[solo_ti] + 1

                              if (ISA(eval_test_ind) and $
                                  (this_inventory[eval_test_ind] eq 1) and $
                                  (eval_test_name eq 'streak')) then begin

                                  prev_hours = 360

                                  PRINT, '"' + eval_test_name + $
                                         '" test likely false alarm:'
                                  PRINT, '  ' + $
                                         site_str + '(' + $
                                         STRCRA(station_obj_id) + $
                                         ') - ' + $
                                         time_str + ' -' + $
                                         ' obs ' + obs_str + ', ' + $
                                         ' mdl ' + mdl_str
                                  PRINT, '  qcdb_si = ' + $
                                         STRCRA(qcdb_si) + $
                                         ', qcdb_ti = ' + $
                                         STRCRA(qcdb_ti)
                                      ;; PRINT, '  ', $
                                      ;;        snwd_qc[0:$
                                      ;;                num_hrs_pad_prev + $
                                      ;;                num_hrs_pad_post, $
                                      ;;                qcdb_si]
                                      ; Get obs and QC for 15 days.
                                  ;; obs_date_Julian = $
                                  ;;     qcdb_time_start_Julian + $
                                  ;;     DOUBLE(qcdb_ti) / 24.0D
                                  sample_start_date_Julian = $
                                      obs_date_Julian - $
                                      DOUBLE(prev_hours) / 24.0D
                                  sample_start_YYYYMMDDHH = $
                                      JULIAN_TO_YYYYMMDDHH( $
                                      sample_start_date_Julian)
                                  sample_finish_YYYYMMDDHH = $
                                      JULIAN_TO_YYYYMMDDHH( $
                                      obs_date_Julian)
                                  ;GF/WW START HERE
                                  swe_sample = $
                                      GET_WDB_OBS_SNOW_DEPTH_FOR_RANGE( $
                                      sample_start_YYYYMMDDHH, $
                                      sample_finish_YYYYMMDDHH, $
                                      STATION_OBJ_ID = station_obj_id)
                                  sample_t1_1 = $
                                      (qcdb_ti - prev_hours) > 0
                                  sample_num_hours = $
                                      qcdb_ti - sample_t1_1 + 1
                                  NCDF_VARGET, qcdb_nc_id, $
                                               'snow_depth_qc', $
                                               snwd_sample_qc, $
                                               COUNT = [sample_num_hours, $
                                                        1], $
                                               OFFSET = [sample_t1_1, $
                                                         qcdb_si]
                                      ;if (sample_t1_1 gt 0) then STOP
                                  snwd_sample_str = $
                                      STRCRA(snwd_sample.obs_value_cm)
                                  ind = WHERE(snwd_sample.obs_value_cm eq $
                                              ndv, count)
                                  if (count gt 0) then $
                                      snwd_sample_str[ind] = '-'
                                      ;; print, snwd_sample_str
                                      ;; foo = snwd_sample_str ne '-'
                                  ind = WHERE(snwd_sample_qc ne 0, count)
                                  if (count gt 0) then $
                                      snwd_sample_str[ind] = '-'
                                  if (sample_num_hours gt prev_hours) then $
                                      begin
                                      PRINT, snwd_sample_str
                                      PRINT, obs_str, ' (', $
                                             snwd_sample.station_type + ')'
                                  endif
                              endif

                              if (ISA(eval_test_ind) and $
                                  (this_inventory[eval_test_ind] eq 1) and $
                                  (eval_test_name eq $
                                   'world_record_increase_exceedance')) $
                              then begin

                                  PRINT, '"' + eval_test_name + $
                                         '" test likely false alarm:'

                                  prev_hrs_wrie = 24
                                  SHOW_SNWD_WRIE_DATA, site_str, $
                                                       time_str, $
                                                       obs_str, $
                                                       mdl_str, $
                                                       obs_date_Julian, $
                                                       prev_hrs_wrie, $
                                                       station_obj_id, $
                                                       qcdb_nc_id, $
                                                       qcdb_ti, $
                                                       qcdb_si, $
                                                       ndv, $
                                                       snwd_sample, $
                                                       snwd_sample_qc

                              endif


                              if (ISA(eval_test_ind) and $
                                  (this_inventory[eval_test_ind] eq 1) and $
                                  (eval_test_name eq $
                                   'temperature_consistency')) $
                              then begin

                                  PRINT, '"' + eval_test_name + $
                                         '" test likely false alarm:'

                                  prev_hours_tair = 24
                                  SHOW_SNWD_TAIR_DATA, site_str, $
                                                       time_str, $
                                                       obs_str, $
                                                       mdl_str, $
                                                       obs_date_Julian, $
                                                       prev_hours_tair, $
                                                       station_obj_id, $
                                                       qcdb_nc_id, $
                                                       qcdb_ti, $
                                                       qcdb_si, $
                                                       ndv, $
                                                       snwd_sample, $
                                                       snwd_sample_qc

                              endif

                              if (ISA(eval_test_ind) and $
                                  (this_inventory[eval_test_ind] eq 1) and $
                                  (eval_test_name eq $
                                   'snowfall_consistency')) $
                              then begin

                                  prev_hours = 24

                                  PRINT, '"' + eval_test_name + $
                                         '" test likely false alarm:'
                                  PRINT, '  ' + $
                                         site_str + $
                                         '(' + STRCRA(station_obj_id) + $
                                         ') - ' + $
                                         time_str + ' -' + $
                                         ' obs ' + obs_str_in + ', ' + $
                                         ' mdl ' + mdl_str_in
                                  PRINT, '  qcdb_si = ' + $
                                         STRCRA(qcdb_si) + $
                                         ', qcdb_ti = ' + $
                                         STRCRA(qcdb_ti)
                                  sample_start_date_Julian = $
                                      obs_date_Julian - $
                                      DOUBLE(prev_hours) / 24.0D
                                  sample_start_YYYYMMDDHH = $
                                      JULIAN_TO_YYYYMMDDHH( $
                                      sample_start_date_Julian)
                                  sample_finish_YYYYMMDDHH = $
                                      JULIAN_TO_YYYYMMDDHH( $
                                      obs_date_Julian)
                                  snwd_sample = $
                                      GET_WDB_OBS_SNOW_DEPTH_FOR_RANGE( $
                                      sample_start_YYYYMMDDHH, $
                                      sample_finish_YYYYMMDDHH, $
                                      STATION_OBJ_ID = station_obj_id)
                                  sample_t1_1 = $
                                      (qcdb_ti - prev_hours) > 0
                                  sample_num_hours = $
                                      qcdb_ti - sample_t1_1 + 1
                                  NCDF_VARGET, qcdb_nc_id, $
                                               'snow_depth_qc', $
                                               snwd_sample_qc, $
                                               COUNT = [sample_num_hours, $
                                                        1], $
                                               OFFSET = [sample_t1_1, $
                                                         qcdb_si]
                                      ;if (sample_t1_1 gt 0) then STOP
                                  snwd_sample_str = $
                                      STRCRA(snwd_sample.obs_value_cm / 2.54)
                                  ind = WHERE(snwd_sample.obs_value_cm eq $
                                              ndv, count)
                                  if (count gt 0) then $
                                      snwd_sample_str[ind] = '-'
                                  ind = WHERE(snwd_sample_qc ne 0, count)
                                  if (count gt 0) then $
                                      snwd_sample_str[ind] = '-'
                                  if (sample_num_hours gt prev_hours) then $
                                      begin
                                      PRINT, snwd_sample_str
                                      PRINT, obs_str_in, ' (', $
                                             snwd_sample.station_type + ')'
                                  endif
                                  move = GET_KBRD(1)

                              endif

                              count_fa++

                          end
                          
                          (abs_diff[wdb_si] ge abs_diff_thresh_cm[0]) and $
                              (abs_diff[wdb_si] lt abs_diff_thresh_cm[1]): begin
                              ; possible false alarm
                              inventory_pfa = inventory_pfa + this_inventory
                              if (solo_ti ne -1) then $
                                  solo_inventory_pfa[solo_ti] = $
                                  solo_inventory_pfa[solo_ti] + 1

                              if (ISA(eval_test_ind) and $
                                  (this_inventory[eval_test_ind] eq 1) and $
                                  (eval_test_name eq $
                                   'world_record_increase_exceedance')) $
                              then begin

                                  PRINT, '"' + eval_test_name + $
                                         '" test possible false alarm:'

                                  prev_hrs_wrie = 24
                                  SHOW_SNWD_WRIE_DATA, site_str, $
                                                       time_str, $
                                                       obs_str, $
                                                       mdl_str, $
                                                       obs_date_Julian, $
                                                       prev_hrs_wrie, $
                                                       station_obj_id, $
                                                       qcdb_nc_id, $
                                                       qcdb_ti, $
                                                       qcdb_si, $
                                                       ndv, $
                                                       snwd_sample, $
                                                       snwd_sample_qc

                              endif

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

                  ;endif
                  ;//

              ;endif
              ;//

          endfor ; loop over obs at current date_Julian

          date_Julian = date_Julian + DOUBLE(cluster_gap_hours) / 24.0D

      endfor ; loop over dates in the current cluster

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

  endwhile ; loop over clusters


;; SKIP:
;;   if full_run then SAVE, /ALL, FILENAME = sav_file

;;   true
;; ; For plotting.
;;   char_size = 1.0
;;   sym_size = 1.0
;;   other_col = 80
;;   fill_col = 220
;;   far_sym = -5
;;   pfar_sym = -8
;;   pos = [0.1, 0.25, 0.9, 0.75]
;;   USERSYM, [-1, 1, 0, -1], [1, 1, -1, 1], THICK = 2

;; ; Show a time series of all false alarm ratios.

;;   ti = 0

;;   for tc = 0, N_ELEMENTS(qc_test_names) - 1 do begin

;;       if (qc_test_names[tc] eq 'naught') then CONTINUE
;;       if (qc_test_names[tc] eq 'anomaly') then CONTINUE
;;       if (qc_test_names[tc] eq 'rate') then CONTINUE

;;       dummy = LABEL_DATE(DATE_FORMAT = '%Y!C%M-%D')

;;       ind = WHERE((pFar[*, ti] ne -1.0) and $
;;                   (FAR[*, ti] ne -1.0) and $
;;                   (solo_freq[*, ti] ne -1.0), count)
;;       if (count eq 0) then begin
;;           PRINT, 'No results for "' + qc_test_names[tc] + '" test'
;;           ti++
;;           CONTINUE
;;       endif

;;       TVLCT, red, grn, blu, /GET
;;       red[other_col] = 150
;;       grn[other_col] = 0
;;       blu[other_col] = 0

;;       SET_PLOT, 'PS'
;;       plot_file = 'eval_snwd_qc' + $
;;                   '_' + start_date_YYYYMMDDHH + '_to_' + $
;;                   finish_date_YYYYMMDDHH + $
;;                   '_' + qc_test_names[tc]
;;       DEVICE, /COLOR, FILE = plot_file + '.ps'
;;       TVLCT, red, grn, blu
;;       !P.Font = 1 ; TrueType
;;       DEVICE, SET_FONT = 'DejaVuSans', /TT_FONT

;;       PLOT, cluster_mean_date_Julian[ind], $
;;             pFAR[ind, ti], $
;;             THICK = 2, XTHICK = 2, YTHICK = 2, CHARTHICK = 2, $
;;             YRANGE = [0.0, 1.0], $
;;             ;; PSYM = pfar_sym, $
;;             TITLE = 'FAR for v0.1.0 "' + qc_test_names[tc] + '" test!C ', $
;;             XTICKFORMAT = 'LABEL_DATE', XTICKUNITS = 'Time', $
;;             YTITLE = 'False Alarm Ratio', $
;;             POS  = pos, $
;;             YSTYLE = 8, $
;;             CHARSIZE = char_size, $
;;             /NODATA
;; ;            SYMSIZE = sym_size, $
;; ;            /NOCLIP

;;       POLYFILL, [cluster_mean_date_Julian[ind[0]], $
;;                  cluster_mean_date_Julian[ind], $
;;                  REVERSE(cluster_mean_date_Julian[ind])], $
;;                 [FAR[ind[0], ti], $
;;                  pFAR[ind, ti], $
;;                  REVERSE(FAR[ind, ti])], $
;;                 COLOR = fill_col

;;       OPLOT, cluster_mean_date_Julian[ind], pFAR[ind, ti], $
;;              PSYM = pfar_sym, THICK = 2, SYMSIZE = sym_size, /NOCLIP

;;       OPLOT, cluster_mean_date_Julian[ind], FAR[ind, ti], $
;;              PSYM = far_sym, THICK = 2, SYMSIZE = sym_size, /NOCLIP

;;       PLOT, cluster_mean_date_Julian, $
;;             num_flagged_obs[*, ti], $
;;             POS = pos, $
;;             XTICKUNITS = 'Time', $
;;             XSTYLE = 4, YSTYLE = 4, $
;;             /NODATA, /NOERASE

;;       OPLOT, cluster_mean_date_Julian, $
;;              num_flagged_obs[*, ti], $
;;              THICK = 2, LINESTYLE = 2, COLOR = other_col, /NOCLIP

;;       AXIS, YAXIS = 1, YTITLE = '# Flagged', CHARSIZE = char_size, $
;;             YTHICK = 2, CHARTHICK = 2, COLOR = other_col

;; ;     Legend.
;;       x1Leg = 0.62
;;       x2Leg = 0.72
;;       yLeg = 0.70
;;       yNudge = 0.01
;;       xBreak = 0.02
;;       yBreak = 0.05
;;       POLYFILL, [x1Leg, x1Leg, x2Leg, x2Leg, x1Leg], $
;;                 [yLeg - yBreak, yLeg, yLeg, yLeg - yBreak, yLeg - yBreak], $
;;                 COLOR = fill_col, /NORMAL
;;       PLOTS, [x1Leg, x2Leg], [yLeg, yLeg], /NORMAL, $
;;              PSYM = pfar_sym, SYMSIZE = sym_size, THICK = 2
;;       XYOUTS, x2Leg + xBreak, yLeg - yNudge, 'Possible FAR', /NORMAL, $
;;               CHARSIZE = char_size, CHARTHICK = 2
;;       yLeg = yLeg - yBreak
;;       PLOTS, [x1Leg, x2Leg], [yLeg, yLeg], /NORMAL, $
;;              PSYM = far_sym, SYMSIZE = sym_size, THICK = 2
;;       XYOUTS, x2Leg + xBreak, yLeg - yNudge, 'Likely FAR', /NORMAL, $
;;               CHARSIZE = char_size, CHARTHICK = 2
;;       yLeg = yLeg - yBreak
;;       PLOTS, [x1Leg, x2Leg], [yLeg, yLeg], /NORMAL, $
;;              LINESTYLE = 2, COLOR = other_col, THICK = 2
;;       XYOUTS, x2Leg + xBreak, yLeg - yNudge, '# Flagged', /NORMAL, $
;;               CHARSIZE = char_size, CHARTHICK = 2, COLOR = other_col

;;       DEVICE, /CLOSE
;;       cmd = 'pstopng ' + plot_file + '.ps'
;;       SPAWN, cmd, EXIT_STATUS = status
;;       if (status ne 0) then STOP
;;       cmd = 'mogrify -trim -border 4% -bordercolor white ' + plot_file + '.png'
;;       SPAWN, cmd, EXIT_STATUS = status
;;       if (status ne 0) then STOP

;;       ;; SET_PLOT, 'X'
;;       ;; WSET_OR_WINDOW, 2
;;       SET_PLOT, 'PS'
;;       plot_file = 'eval_snwd_qc' + $
;;                   '_' + start_date_YYYYMMDDHH + '_to_' + $
;;                   finish_date_YYYYMMDDHH + $
;;                   '_' + qc_test_names[tc] + '_solo'
;;       DEVICE, /COLOR, FILE = plot_file + '.ps'
;;       TVLCT, red, grn, blu
;;       !P.Font = 1 ; TrueType
;;       DEVICE, SET_FONT = 'DejaVuSans', /TT_FONT

;;       PLOT, cluster_mean_date_Julian[ind], $
;;             solo_freq[ind, ti], $
;;             THICK = 2, XTHICK = 2, YTHICK = 2, CHARTHICK = 2, $
;;             YRANGE = [0.0, 1.0], $
;;             LINESTYLE = 3, $
;; ;            PSYM = far_sym, $ 
;;             TITLE = 'Solo Frequency / FAR for v0.1.0 "' + $
;;                     qc_test_names[tc] + '" test!C ', $
;;             XTICKFORMAT = 'LABEL_DATE', XTICKUNITS = 'Time', $
;;             YTITLE = 'Solo Frequency / FAR', $
;;             POS = pos, $
;;             YSTYLE = 8, $
;;             CHARSIZE = char_size, $
;;             SYMSIZE = sym_size, $
;;             /NOCLIP

;;       ; num_solo_flagged_obs / solo_freq = num_flagged_obs

;;       ind = WHERE((solo_FAR[*, ti] ne -1.0) and $
;;                   (solo_pFAR[*, ti] ne -1.0), count)
;;       if (count ne 0) then begin
;;           POLYFILL, [cluster_mean_date_Julian[ind[0]], $
;;                      cluster_mean_date_Julian[ind], $
;;                      REVERSE(cluster_mean_date_Julian[ind])], $
;;                     [solo_FAR[ind[0], ti], $
;;                      solo_pFAR[ind, ti], $
;;                      REVERSE(solo_FAR[ind, ti])], $
;;                     COLOR = fill_col
;;           OPLOT, cluster_mean_date_Julian[ind], $
;;                  solo_FAR[ind, ti], $
;;                  PSYM = far_sym, THICK = 2, SYMSIZE = sym_size, /NOCLIP
;;           OPLOT, cluster_mean_date_Julian[ind], $
;;                  solo_pFAR[ind, ti], $
;;                  PSYM = pfar_sym, THICK = 2, SYMSIZE = sym_size, /NOCLIP
;;       endif

;;       PLOT, cluster_mean_date_Julian, $
;;             num_solo_flagged_obs[*, ti], $
;;             POS = pos, $
;;             XTICKUNITS = 'Time', $
;;             XSTYLE = 4, YSTYLE = 4, $
;;             /NODATA, /NOERASE

;;       OPLOT, cluster_mean_date_Julian, $
;;              num_solo_flagged_obs[*, ti], $
;;              THICK = 2, LINESTYLE = 2, COLOR = other_col

;;       AXIS, YAXIS = 1, YTITLE = '# Solo-Flagged', CHARSIZE = char_size, $
;;             YTHICK = 2, CHARTHICK = 2, COLOR = other_col

;; ;     Legend
;;       x1Leg = 0.58
;;       x2Leg = 0.68
;;       yLeg = 0.70
;;       yNudge = 0.01
;;       xBreak = 0.02
;;       yBreak = 0.05
;;       POLYFILL, [x1Leg, x1Leg, x2Leg, x2Leg, x1Leg], $
;;                 [yLeg - yBreak, yLeg, yLeg, yLeg - yBreak, yLeg - yBreak], $
;;                 COLOR = fill_col, /NORMAL
;;       PLOTS, [x1Leg, x2Leg], [yLeg, yLeg], /NORMAL, $
;;              PSYM = pfar_sym, SYMSIZE = sym_size, THICK = 2
;;       XYOUTS, x2Leg + xBreak, yLeg - yNudge, 'Possible Solo FAR', /NORMAL, $
;;               CHARSIZE = char_size, CHARTHICK = 2
;;       yLeg = yLeg - yBreak
;;       PLOTS, [x1Leg, x2Leg], [yLeg, yLeg], /NORMAL, $
;;              PSYM = far_sym, SYMSIZE = sym_size, THICK = 2
;;       XYOUTS, x2Leg + xBreak, yLeg - yNudge, 'Likely Solo FAR', /NORMAL, $
;;               CHARSIZE = char_size, CHARTHICK = 2
;;       yLeg = yLeg - yBreak
;;       PLOTS, [x1Leg, x2Leg], [yLeg, yLeg], /NORMAL, $
;;              LINESTYLE = 3, THICK = 2
;;       XYOUTS, x2Leg + xBreak, yLeg - yNudge, 'Solo Frequency', /NORMAL, $
;;               CHARSIZE = char_size, CHARTHICK = 2
;;       yLeg = yLeg - yBreak
;;       PLOTS, [x1Leg, x2Leg], [yLeg, yLeg], /NORMAL, $
;;              LINESTYLE = 2, COLOR = other_col, THICK = 2
;;       XYOUTS, x2Leg + xBreak, yLeg - yNudge, '# Solo-Flagged', /NORMAL, $
;;               CHARSIZE = char_size, CHARTHICK = 2, COLOR = other_col

;;       DEVICE, /CLOSE
;;       cmd = 'pstopng ' + plot_file + '.ps'
;;       SPAWN, cmd, EXIT_STATUS = status
;;       if (status ne 0) then STOP
;;       cmd = 'mogrify -trim -border 4% -bordercolor white ' + plot_file + '.png'
;;       SPAWN, cmd, EXIT_STATUS = status
;;       if (status ne 0) then STOP

;;       ;; if (tc ne N_ELEMENTS(qc_test_names) - 1) then $
;;       ;;     move = GET_KBRD(1)

;;       ti++

;;   endfor

  ;; if full_run then
  NCDF_CLOSE, qcdb_nc_id

end
