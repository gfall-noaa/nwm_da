FUNCTION DATE_TO_JULIAN, dateTime

; Convert "YYYY-MM-DD HH:MM:SS" to Julian date.

  year = FIX(STRMID(dateTime, 0, 4))
  month = FIX(STRMID(dateTime, 5, 2))
  day = FIX(STRMID(dateTime, 8, 2))
  hour = FIX(STRMID(dateTime, 11, 2))
  minute = FIX(STRMID(dateTime, 14, 2))
  second = FIX(STRMID(dateTime, 17, 2))

  RETURN, JULDAY(month, day, year, hour, minute, second)

end
