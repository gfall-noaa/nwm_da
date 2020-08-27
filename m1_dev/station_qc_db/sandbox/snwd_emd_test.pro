; Program to test empirical mode decomposition (EMD) to fit trends to
; observed snow depth time series.

; The original purpose is to quantify the random error fluctuations in
; noisy snow depth reports, but fitting trends to observed time series
; and computing residuals could have many applications, not just for
; quality control.

; Zhaohua Wu, Norden E. Huang, Steven R. Long, and Chung-Kang Peng
; On the trend, detrending, and variability of nonlinear and nonstationary
; time series
; https://doi.org/10.1073/pnas.0701020104
; https://pnas.org/content/104/38/14889
;
; https://en.wikipedia.org/wiki/Hilbert-Huang_transform

  start_date_YYYYMMDDHH = '2019120100'
  finish_date_YYYYMMDDHH = '2020022923'
  ndv = -99999.0
  scratch_dir = '/net/scratch/nwm_snow_da/wdb0_sav'
  
  snwd = GET_WDB_OBS_SNOW_DEPTH_FOR_RANGE(start_date_YYYYMMDDHH, $
                                          finish_date_YYYYMMDDHH, $
                                          SCRATCH_DIR = scratch_dir, $
                                          NDV = ndv)

  ids = ['LVTC1', 'KOSQ2', 'BRTW1']

  dummy = LABEL_DATE(DATE_FORMAT = '%Y!C%N-%D')

  for sc = 0, N_ELEMENTS(snwd) - 1 do begin

      ;; ind = WHERE(snwd[sc].station_id eq ids, count)
      ;; if (count eq 0) then CONTINUE

      ind = WHERE(snwd[sc].obs_value_cm ne ndv, count)
      if (count eq 0) then STOP

      WSET_OR_WINDOW, 0, XSIZE = 1600, YSIZE = 900

      PLOT, snwd[sc].date_utc_julian[ind], snwd[sc].obs_value_cm[ind], $
            PSYM = -4, $
            TITLE = snwd[sc].station_id + ' ' + $
                    start_date_YYYYMMDDHH + ' to ' + finish_date_YYYYMMDDHH, $
            XTICKFORMAT = 'LABEL_DATE', XTICKUNITS = 'Time', $
            THICK = 2, $
            /NOCLIP, $
            CHARSIZE = 1.5, $
            XRANGE = [snwd[sc].date_utc_julian[ind[0]], $
                      snwd[sc].date_utc_julian[ind[count-1]]], $
            XSTYLE = 1, XMINOR = 1
; EMD START
  x = snwd[sc].date_utc_julian[ind]
  y = snwd[sc].obs_value_cm[ind]
  n = N_ELEMENTS(x)
  if (N_ELEMENTS(y) ne n) then STOP
  if (n lt 3) then CONTINUE
  sort_ind = sort(x)
  x_sorted = x[sort_ind]
  y_sorted = y[sort_ind]

; Find local maxima and minima.
  ;; min_ind = LCLXTREM(y_sorted, 1, COUNT = min_count, /POINT_ORDER)
  ;; max_ind = LCLXTREM(y_sorted, 1, /MAXIMA, COUNT = max_count, /POINT_ORDER)

  extrema_ind = EXTREMA(y_sorted, /FLAT, MINIMA = min_ind, MAXIMA = max_ind)
  max_count = N_ELEMENTS(max_ind)
  min_count = N_ELEMENTS(min_ind)

  ;; maxima_ind = WHERE((y_sorted[1:n-2] gt y_sorted[0:n-3]) and $
  ;;                    (y_sorted[1:n-2] gt y_sorted[2:n-1]), maxima_count)
  ;; if (maxima_count gt 0) then maxima_ind = maxima_ind + 1
  ;; minima_ind = WHERE((y_sorted[1:n-2] lt y_sorted[0:n-3])and $
  ;;                    (y_sorted[1:n-2] lt y_sorted[2:n-1]), minima_count)
  ;; if (minima_count gt 0) then min_ind = min_ind + 1

  TVLCT, red, grn, blu, /GET
  red_ind = 1
  red[red_ind] = 255
  grn[red_ind] = 150
  blu[red_ind] = 150
  blu_ind = 2
  red[blu_ind] = 150
  grn[blu_ind] = 150
  blu[blu_ind] = 255
  grn_ind = 3
  red[grn_ind] = 150
  grn[grn_ind] = 255
  blu[grn_ind] = 150
  
  TVLCT, red, grn, blu

  for xc = 0, max_count - 1 do begin
      i = max_ind[xc]
      PLOTS, x_sorted[i], y_sorted[i], PSYM = 5, SYMSIZE = 2, $
             COLOR = blu_ind, THICK = 2
  endfor
  for nc = 0, min_count - 1 do begin
      i = min_ind[nc]
      PLOTS, x_sorted[i], y_sorted[i], PSYM = 6, SYMSIZE = 2, $
             COLOR = red_ind, THICK = 2
  endfor

  ;; y_str = STRCRA(y_sorted)
  ;; for i = 0, n - 1 do begin
  ;;     if (min_count gt 0) then begin
  ;;         ind = WHERE(min_ind eq i, count)
  ;;         if (count gt 0) then begin
  ;;             if (count ne 1) then STOP
  ;;             y_str[i] = y_str[i] + '(min)'
  ;;         endif
  ;;     endif
  ;;     if (max_count gt 0) then begin
  ;;         ind = WHERE(max_ind eq i, count)
  ;;         if (count gt 0) then begin
  ;;             if (count ne 1) then STOP
  ;;             y_str[i] = y_str[i] + '(max)'
  ;;         endif
  ;;     endif
  ;; endfor

  if ((min_count ge 3) and (max_count ge 3)) then begin

      m_lo = INTERPOL(y_sorted[min_ind], x_sorted[min_ind], x_sorted, $
                      /QUAD)
      m_hi = INTERPOL(y_sorted[max_ind], x_sorted[max_ind], x_sorted, $
                      /QUAD)
      ;; m_lo = SPLINE(x_sorted[min_ind], y_sorted[min_ind], x_sorted)
      ;; m_hi = SPLINE(x_sorted[max_ind], y_sorted[max_ind], x_sorted)
      m1 = 0.5 * (m_lo + m_hi)

      OPLOT, x_sorted, m_lo, LINESTYLE = 2, COLOR = red_ind, THICK = 2, $
             /NOCLIP
      OPLOT, x_sorted, m_hi, LINESTYLE = 2, COLOR = blu_ind, THICK = 2, $
             /NOCLIP
      OPLOT, x_sorted, m1, COLOR = grn_ind, THICK = 2, $
             /NOCLIP
  endif
  move = GET_KBRD(1)





  ;;     h1 = y_sorted - m1

  ;;     WSET_OR_WINDOW, 2, XSIZE = 1600, YSIZE = 900
  ;;     PLOT, x_sorted, h1, $
  ;;           TITLE = snwd[sc].station_id + ' ' + $
  ;;                   start_date_YYYYMMDDHH + ' to ' + finish_date_YYYYMMDDHH, $
  ;;           XTICKFORMAT = 'LABEL_DATE', XTICKUNITS = 'Time', $
  ;;           /NOCLIP, $
  ;;           THICK = 2, $
  ;;           PSYM = -4
  ;;           CHARSIZE = 1.5

  ;;     OPLOT, !X.CRange, [0.0, 0.0], LINESTYLE = 2, THICK = 2

  ;;     ; Next iteration
  ;;     k = 1

      
  ;;     PRINT, '>', y_str
  ;;     move = GET_KBRD(1)

  ;; endif

  imf = GF_EMD(y_sorted, /FLAT, /I_QUAD)
;  imf = EMD(y_sorted, /QUEK)
  size_imf = SIZE(imf)
  if (size_imf[0] ne 2) then begin
      if (size_imf[0] ne 1) then STOP
      ;; PRINT, 'trivial solution'
      num_imf = 1
      num_imf_str = '1 IMF'
  endif else begin
      num_imf = size_imf[2]
      num_imf_str = STRCRA(num_imf) + ' IMFs'
  endelse
  if (size_imf[1] ne n) then STOP

; Reconstruct the time series starting with the kth IMF. Note that
; yk[*, 0] is the same as y_sorted.
  ;; if (num_imf gt 1) then begin
  ;;     yk = FLTARR(n, num_imf)
  ;;     for k = 0, num_imf - 1 do $
  ;;         yk[*, k] = imf[*, k:num_imf - 1] # REPLICATE(1.0, num_imf - k)
  ;;     CMSE = FLTARR(num_imf - 1)
  ;;     for k = 0, num_imf - 2 do begin
  ;;         CMSE[k] = MEAN((yk[*,k] - yk[*,k+1])^2.0)
  ;;     endfor
  ;;     min_CMSE = MIN(CMSE, js) & js++
  ;;     PRINT, cmse
  ;;     PRINT, 'js = ' + STRCRA(js) + ', C = ' + STRCRA(num_imf)
  ;; endif

; Assume all noise is in first IMF.
  noise = imf[*, 0]
  signal = !NULL
  if (num_imf eq 1) then begin
      noise = !NULL
      signal = imf[*, 0]
  endif else begin
      noise = imf[*, 0]
      signal = imf[*, 1:num_imf - 1] # replicate(1.0, num_imf - 1)
  endelse
  WSET_OR_WINDOW, 0
  PLOT, snwd[sc].date_utc_julian[ind], snwd[sc].obs_value_cm[ind], $
        TITLE = snwd[sc].station_id + ' ' + $
        start_date_YYYYMMDDHH + ' to ' + finish_date_YYYYMMDDHH, $
        XTICKFORMAT = 'LABEL_DATE', XTICKUNITS = 'Time', $
        CHARSIZE = 1.5, $
        XRANGE = [snwd[sc].date_utc_julian[ind[0]], $
                  snwd[sc].date_utc_julian[ind[count-1]]], $
        XSTYLE = 1, XMINOR = 1, /NODATA, /NOERASE 
  OPLOT, x_sorted, signal, COLOR = grn_ind, THICK = 3, LINESTYLE = 2, /NOCLIP
;  OPLOT, x_sorted, imf[*, num_imf - 1], COLOR = blu_ind, THICK = 3, /NOCLIP
  if ISA(NOISE) then begin
      OPLOT, x_sorted, noise, COLOR = red_ind, THICK = 2, /NOCLIP
      PRINT, 'noise: ', TOTAL(noise), MEAN(noise), STDDEV(noise)
  endif

  WSET_OR_WINDOW, 2, XSIZE = 1600, YSIZE = 900
  PLOT, x_sorted, imf[*, 0], $
        TITLE = snwd[sc].station_id + ' ' + $
        start_date_YYYYMMDDHH + ' to ' + finish_date_YYYYMMDDHH + $
        ', ' + num_imf_str, $
        XTICKFORMAT = 'LABEL_DATE', XTICKUNITS = 'Time', $
        /NODATA, $
        /NOCLIP, $
        THICK = 2, $
        CHARSIZE = 1.5, $
        YRANGE = [-MAX(ABS(imf)), MAX(ABS(imf))], $
        XRANGE = [snwd[sc].date_utc_julian[ind[0]], $
                  snwd[sc].date_utc_julian[ind[count-1]]], $
        XSTYLE = 1, XMINOR = 1
  OPLOT, !X.CRange, [0.0, 0.0], LINESTYLE = 2
  for ic = 0, num_imf - 1 do begin
      OPLOT, x_sorted, imf[*, ic], $
             ;; PSYM = -((ic mod 7) + 1), $
             ;; LINESTYLE = (ic mod 5) + 1, $
             COLOR = 255 / num_imf * (ic + 1), $
             THICK = 2
  endfor
  if ISA(noise) then OPLOT, x_sorted, noise, COLOR = red_ind, THICK = 2
  move = GET_KBRD(1)
  
; EMD FINISH
      
  endfor
  
end
