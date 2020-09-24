FUNCTION GET_WDB_OBS_SWE_FOR_RANGE, start_date_YYYYMMDDHH, $
                                    finish_date_YYYYMMDDHH, $
                                    NDV = ndv_, $
                                    STATION_OBJ_ID = station_obj_id, $
                                    SCRATCH_DIR = scratch_dir

; Adapted from GET_WDB_OBS_SNOW_DEPTH_FOR_RANGE

  swe_report = !NULL


; Check arguments for correct type and valid contents.

  if NOT(ISA(start_date_YYYYMMDDHH, 'STRING')) then begin
      ERR_MSG, 'Start date/time argument must be a STRING.'
      RETURN, swe_report
  endif
  if (STRLEN(start_date_YYYYMMDDHH) ne 10) then begin
      ERR_MSG, 'Invalid start date/time "' + $
               start_date_YYYYMMDDHH + $
               '" (required form is YYYYMMDDHH, 10 digits).'
      RETURN, swe_report
  endif
  if NOT(STREGEX(start_date_YYYYMMDDHH, '[0-9]{10}', /BOOLEAN)) $
      then begin
      ERR_MSG, 'Invalid start date/time "' + $
               start_date_YYYYMMDDHH + $
               '" (required form is YYYYMMDDHH, all numeric).'
      RETURN, swe_report
  endif

  if NOT(ISA(finish_date_YYYYMMDDHH, 'STRING')) then begin
      ERR_MSG, 'Observation finish date/time argument must be a STRING.'
      RETURN, swe_report
  endif
  if (STRLEN(finish_date_YYYYMMDDHH) ne 10) then begin
      ERR_MSG, 'Invalid finish date/time "' + $
               finish_date_YYYYMMDDHH + $
               '" (required form is YYYYMMDDHH, 10 digits).'
      RETURN, swe_report
  endif
  if NOT(STREGEX(finish_date_YYYYMMDDHH, '[0-9]{10}', /BOOLEAN)) $
      then begin
      ERR_MSG, 'Invalid finish date/time "' + $
               finish_date_YYYYMMDDHH + $
               '" (required form is YYYYMMDDHH, all numeric).'
      RETURN, swe_report
  endif
  
  start_date_Julian = YYYYMMDDHH_TO_JULIAN(start_date_YYYYMMDDHH)
  finish_date_Julian = YYYYMMDDHH_TO_JULIAN(finish_date_YYYYMMDDHH)

  lag = SYSTIME(/JULIAN, /UTC) - finish_date_Julian

  save_file_name = 'wdb0_obs_swe_' + $
                   start_date_YYYYMMDDHH + $
                   '_to_' + $
                   finish_date_YYYYMMDDHH + $
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
          RETURN, swe_report
      endif
  endif

  num_hours = ROUND((finish_date_Julian - start_date_Julian) * 24.0D) + 1L
  date_Julian = start_date_Julian + DINDGEN(num_hours) / 24.0D

  if KEYWORD_SET(ndv_) then $
      ndv = ndv_ $
  else $
      ndv = -99999.0

  start_date = STRMID(start_date_YYYYMMDDHH, 0, 4) + '-' + $
               STRMID(start_date_YYYYMMDDHH, 4, 2) + '-' + $
               STRMID(start_date_YYYYMMDDHH, 6, 2) + ' ' + $
               STRMID(start_date_YYYYMMDDHH, 8, 2) + ':' + $
               '00:00'

  finish_date = STRMID(finish_date_YYYYMMDDHH,  0, 4) + '-' + $
                STRMID(finish_date_YYYYMMDDHH, 4, 2) + '-' + $
                STRMID(finish_date_YYYYMMDDHH, 6, 2) + ' ' + $
                STRMID(finish_date_YYYYMMDDHH, 8, 2) + ':' + $
                '00:00'

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
                  'point.obs_swe as t2 ' + $
                  'where ' + $
                  't2.obj_identifier = ' + STRCRA(station_obj_id) + ' ' + $
                  'and t2.date >= ''' + start_date + ''' ' + $
                  'and t2.date <= ''' + finish_date + ''' ' + $
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
                  'point.obs_swe as t2 ' + $
                  'where ' + $
                  't2.date >= ''' + start_date + ''' ' + $
                  'and t2.date <= ''' + finish_date + ''' ' + $
                  'and t2.value is not NULL ' + $
                  'and t1.obj_identifier = t2.obj_identifier ' + $
                  'order by t2.obj_identifier, t2.date;"'

  endelse

  SPAWN, statement, result, EXIT_STATUS = status
  fetch_date_Julian = SYSTIME(/JULIAN, /UTC)

  if (status ne 0) then begin
      ERR_MSG, 'psql statement failed: ' + statement
      RETURN, swe_report
  endif

; Determine the number of unique stations reporting SWE.

  num_swe = N_ELEMENTS(result)

  if (result[0] eq '') then begin
      num_swe = 0
      RETURN, swe_report
  endif

  prev_obj_id = -1L
  obj_id = !NULL
  for rc = 0, num_swe - 1 do begin
      report = STRSPLIT(result[rc], '|', /EXTRACT)
      if (N_ELEMENTS(report) ne 11) then begin
          ERR_MSG, 'Unrecognized structure in snow depth reports.'
          stop
          num_swe = 0
          RETURN, swe_report
      endif
      this_obj_id = LONG(report[0])
      if (this_obj_id ne prev_obj_id) then begin
          obj_id = [obj_id, this_obj_id]
          prev_obj_id = this_obj_id
      endif
  endfor

  num_stations = N_ELEMENTS(obj_id)

  if (KEYWORD_SET(station_obj_id) and (num_stations gt 1)) then STOP


; Place results in a structure.

  swe_report_ = REPLICATE({station_obj_id: 0L, $
                           station_id: '', $
                           station_name: '', $
                           station_type: '', $
                           station_source: '', $
                           longitude: 0.0D, $
                           latitude: 0.0D, $
                           elevation: 0L, $
                           recorded_elevation: 0L, $
                           date_UTC_Julian: date_Julian, $
                           obs_value_mm: REPLICATE(ndv, num_hours)}, $
                          num_stations)

  prev_obj_id = -1L
  sc = -1L
  for rc = 0, num_swe - 1 do begin
      report = STRSPLIT(result[rc], '|', /EXTRACT)
      if (N_ELEMENTS(report) ne 11) then begin
          ERR_MSG, 'Unrecognized structure in snow depth reports.'
          num_swe = 0
          RETURN, swe_report
      endif
      this_obj_id = LONG(report[0])
      if (this_obj_id ne prev_obj_id) then begin
          sc++
          if (this_obj_id ne obj_id[sc]) then STOP ; PROGRAMMING ERROR
          swe_report_[sc].station_obj_id = LONG(report[0])
          swe_report_[sc].station_id = report[1]
          swe_report_[sc].station_name = report[2]
          swe_report_[sc].station_type = report[3]
          swe_report_[sc].station_source = report[4]
          swe_report_[sc].longitude = DOUBLE(report[5])
          swe_report_[sc].latitude = DOUBLE(report[6])
          swe_report_[sc].elevation = LONG(report[7])
          swe_report_[sc].recorded_elevation = LONG(report[8])
          prev_obj_id = this_obj_id
      endif
      this_date_Julian = DATE_TO_JULIAN(report[9])
      tc = ROUND((this_date_Julian - start_date_Julian) * 24.0D)
      swe_report_[sc].obs_value_mm[tc] = FLOAT(report[10]) * 1000.0
  endfor

  if KEYWORD_SET(station_obj_id) then $
      swe_report_ = swe_report_[0]

  swe_report = TEMPORARY(swe_report_)

  if ((lag gt 60.0D) and $
      KEYWORD_SET(scratch_dir) and $
      NOT(KEYWORD_SET(station_obj_id))) $
      then begin
      ;; if NOT(FILE_TEST(scratch_dir, /DIRECTORY, /WRITE)) then begin
      ;;     ERR_MSG, 'Invalid scratch directory "' + scratch_dir + '".'
      ;;     RETURN, !NULL
      ;; endif
      SAVE, swe_report, ndv, $
            FILE = scratch_dir + '/' + save_file_name
      PRINT, 'Data saved to ' + scratch_dir + '/' + save_file_name
  endif
  
  RETURN, swe_report

end
