FUNCTION GET_WDB_OBS_AIR_TEMP_FOR_RANGE, start_date_YYYYMMDDHH, $
                                         finish_date_YYYYMMDDHH, $
                                         NDV = ndv, $
                                         STATION_OBJ_ID = station_obj_id

  air_temp_report = !NULL


; Check arguments for correct type and valid contents.

  if NOT(ISA(start_date_YYYYMMDDHH, 'STRING')) then begin
      ERR_MSG, 'Start date/time argument must be a STRING.'
      RETURN, air_temp_report
  endif
  if (STRLEN(start_date_YYYYMMDDHH) ne 10) then begin
      ERR_MSG, 'Invalid start date/time "' + $
               start_date_YYYYMMDDHH + $
               '" (required form is YYYYMMDDHH, 10 digits).'
      RETURN, air_temp_report
  endif
  if NOT(STREGEX(start_date_YYYYMMDDHH, '[0-9]{10}', /BOOLEAN)) $
      then begin
      ERR_MSG, 'Invalid start date/time "' + $
               start_date_YYYYMMDDHH + $
               '" (required form is YYYYMMDDHH, all numeric).'
      RETURN, air_temp_report
  endif

  if NOT(ISA(finish_date_YYYYMMDDHH, 'STRING')) then begin
      ERR_MSG, 'Observation finish date/time argument must be a STRING.'
      RETURN, air_temp_report
  endif
  if (STRLEN(finish_date_YYYYMMDDHH) ne 10) then begin
      ERR_MSG, 'Invalid finish date/time "' + $
               finish_date_YYYYMMDDHH + $
               '" (required form is YYYYMMDDHH, 10 digits).'
      RETURN, air_temp_report
  endif
  if NOT(STREGEX(finish_date_YYYYMMDDHH, '[0-9]{10}', /BOOLEAN)) $
      then begin
      ERR_MSG, 'Invalid finish date/time "' + $
               finish_date_YYYYMMDDHH + $
               '" (required form is YYYYMMDDHH, all numeric).'
      RETURN, air_temp_report
  endif

  start_date_Julian = YYYYMMDDHH_TO_JULIAN(start_date_YYYYMMDDHH)
  finish_date_Julian = YYYYMMDDHH_TO_JULIAN(finish_date_YYYYMMDDHH)

  num_hours = ROUND((finish_date_Julian - start_date_Julian) * 24.0D) + 1L
  date_Julian = start_date_Julian + DINDGEN(num_hours) / 24.0D

  if NOT(KEYWORD_SET(ndv)) then ndv = -99999.0

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
                  'point.obs_airtemp as t2 ' + $
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
                  'point.obs_airtemp as t2 ' + $
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
      RETURN, air_temp_report
  endif

; Determine the number of unique stations reporting air temperature.

  num_air_temp = N_ELEMENTS(result)

  if (result[0] eq '') then begin
      num_air_temp = 0
      RETURN, air_temp_report
  endif

  prev_obj_id = -1L
  obj_id = !NULL
  for rc = 0, num_air_temp - 1 do begin
      report = STRSPLIT(result[rc], '|', /EXTRACT)
      if (N_ELEMENTS(report) ne 11) then begin
          ERR_MSG, 'Unrecognized structure in air temperature reports.'
          stop
          num_air_temp = 0
          RETURN, air_temp_report
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

  air_temp_report_ = REPLICATE({station_obj_id: 0L, $
                                station_id: '', $
                                station_name: '', $
                                station_type: '', $
                                station_source: '', $
                                longitude: 0.0D, $
                                latitude: 0.0D, $
                                elevation: 0L, $
                                recorded_elevation: 0L, $
                                date_UTC_Julian: date_Julian, $
                                obs_value_deg_c: REPLICATE(ndv, num_hours)}, $
                               num_stations)

  prev_obj_id = -1L
  sc = -1L
  for rc = 0, num_air_temp - 1 do begin
      report = STRSPLIT(result[rc], '|', /EXTRACT)
      if (N_ELEMENTS(report) ne 11) then begin
          ERR_MSG, 'Unrecognized structure in air temperature reports.'
          num_air_temp = 0
          RETURN, air_temp_report
      endif
      this_obj_id = LONG(report[0])
      if (this_obj_id ne prev_obj_id) then begin
          sc++
          if (this_obj_id ne obj_id[sc]) then STOP ; PROGRAMMING ERROR
          air_temp_report_[sc].station_obj_id = LONG(report[0])
          air_temp_report_[sc].station_id = report[1]
          air_temp_report_[sc].station_name = report[2]
          air_temp_report_[sc].station_type = report[3]
          air_temp_report_[sc].station_source = report[4]
          air_temp_report_[sc].longitude = DOUBLE(report[5])
          air_temp_report_[sc].latitude = DOUBLE(report[6])
          air_temp_report_[sc].elevation = LONG(report[7])
          air_temp_report_[sc].recorded_elevation = LONG(report[8])
          prev_obj_id = this_obj_id
      endif
      this_date_Julian = DATE_TO_JULIAN(report[9])
      tc = ROUND((this_date_Julian - start_date_Julian) * 24.0D)
      air_temp_report_[sc].obs_value_deg_c[tc] = FLOAT(report[10])
  endfor

  if KEYWORD_SET(station_obj_id) then $
      air_temp_report_ = air_temp_report_[0]

  air_temp_report = TEMPORARY(air_temp_report_)

  RETURN, air_temp_report

end
