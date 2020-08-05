;+
; NAME:
;	EMD
;
; PURPOSE:
;	This function estimates the empirical mode decomposition of a given 
;	data vector.
;
; CATEGORY:
;	Time series analysis
;
; CALLING SEQUENCE:
;	Result = EMD( Data )
;
; INPUT:
;	Data:  A floating point vector containing the input data values.
;
; KEYWORD PARAMETERS:
;	QUEK:  If set, the procedure test for IMFs by checking the size of
;		the difference between successive rounds but with a modified
;		comparison as adopted by Quek et alii (2003).  The default is 
;		without the modification.
;	SHIFTFACTOR:  A floating point factor to be used in comparing 
;		normalised squared differences between successive rounds when 
;		testing for IMFs.  The default is 0.3.
;	SPLINEMEAN:  If set, the procedure estimates the local mean by splining
;		between the mean of the extrema.  The default is to take the 
;		mean of the splines through the extrema.
;	VERBOSE:  If set, the procedure prints some output and error warnings.
;		The default is not set.
;	ZEROCROSS:  If set, the procedure tests for IMFs by comparing the 
;		number of extrema and zero crossings.  The default is by 
;		checking the size of the difference between successive rounds.
;
; OUTPUT:
;	Result:  Returns a floating point matrix containing the intrinsic mode 
;		functions (IMFs).  The dimensions are number of time steps by 
;		number of IMFs.
;
; USES:
;	EXTREMA.pro
;	VAR_TYPE.pro
;	ZERO_CROSS.pro
;
; PROCEDURE:
;	This function uses an iterative method to decompose a time series into 
;	intrinsic mode functions according to Huang et alii.
;
; EXAMPLE:
;	Given a time series DATA.
;	  imf = EMD( data )
;       Plot the data.
;	  plot, data
;	Overplot the sum of the IMFs.  This should be identical to DATA.
;	  tek_color
;	  oplot, total( imf, 2 ), color=2
;
; REFERENCES:
;	Huang et al, Royal Society Proceedings on Math, Physical, 
;	  and Engineering Sciences, vol. 454, no. 1971, pp. 903-995, 
;	  8 March 1998
;	Coughlin et al. (2003), in press.
;	Quek, et al. (2003), Smart Mater. Struct., 12, 447-460.
;
; MODIFICATION HISTORY:
; 	MatLab:	Ivan Magrin-Chagnolleau (ivan@ieee.org)
;	Matlab:	Anthony Wilson (anthony.wilson:zoo.ox.ac.uk), 2003
;	Written by:	Daithi A. Stone (stoned@atm.ox.ac.uk), 2003-12-18
;			(adapted MatLab to IDL)
;	Modified:	DAS, 2004-07-27 (documentation for routine library)
;	Modified:	DAS, 2005-08-05 (replaced sum_row.pro with total)
;-

FUNCTION EMD, $
	Data, $
	SHIFTFACTOR=shiftfactor, $
	QUEK=quekopt, SPLINEMEAN=splinemeanopt, ZEROCROSS=zerocrossopt, $
	VERBOSE=verboseopt

;***********************************************************************
; Constants and Options

; Set factor for dealing with numerical precision
epsilon = 0.00001

; Set number of shifting iterations to ensure stable IMF upon IMF candidate
; detection.
ncheckimf = 3

; Set factor limiting the normalised standard deviation between consecutive 
; shifts in the IMF calculation.  Used if ZEROCROSS is not set.
if not( keyword_set( shiftfactor ) ) then shiftfactor = 0.3

; Initialise check variable for determining loop exit.
; Check = 0 means that we have nothing yet.
check = 0
; Check = 1 means that we have an IMF.
checkimfval = 1
; Check = 2 means that we have the residual.
checkresval = 2
; Check = 3 means that we exit the program.
checkexitval = 3

; Length of the time series
ndata = n_elements( data )

; We need to know if we need to use long integers for indices
if var_type( ndata ) eq 2 then begin
  idtype = 1
endif else begin
  idtype = 1l
endelse

; Options
quekopt = keyword_set( quekopt )
splinemeanopt = keyword_set( splinemeanopt )
zerocrossopt = keyword_set( zerocrossopt )

; Option to print output and error warnings.
verboseopt = keyword_set( verboseopt )

; Initialise the vector to be decomposed (it is altered with each step)
x = data

;***********************************************************************
; Decompose the Input Vector into Its IMFs

; Iterate until signal has been decomposed
while check lt checkexitval do begin

  ; Check if we have extracted everything (ie if you have the residual).
  ; Find local extrema for minimum and maximum envelopes.
  nextrema = n_elements( extrema( x ) )
  ; Check for at least 1 extremum.
  if nextrema le 2 then check = checkresval
  ; Check for very small residual.
  if stddev( x ) lt epsilon * stddev( data ) then check = checkresval

  ; Remember what x was
  x0 = x

  ; Initialise checkimf variable for determining stable IMF
  checkimf = 0
  checkres = 0

  ; Iterate while the IMF criterion is not yet reached.
  ; These criteria are incorporated into the Check variable.
  while check eq 0 do begin

    ; Find local extrema for minimum and maximum envelopes
    ;temp = extrema( x, minima=minima, maxima=maxima, /flat )
    temp = extrema( x, minima=minima, maxima=maxima )
    nminima = n_elements( minima )
    nmaxima = n_elements( maxima )

    ; Add a constant extension to the ends of the maxima and minima vectors.
    ; This is to get a better spline fit at the ends.
    ; This is done by adding two cycles of a wave of wavelength 
    ; 2*abs(maxima[0]-minima[0]) onto the beginning, and similarly for the end.

    ; Period of beginning wave
    period0 = 2 * abs ( maxima[0] - minima[0] )
    ; Period of end wave
    period1 = 2 * abs( maxima[nmaxima-1] - minima[nminima-1] )

    ; Extend the extrema vectors
    maxpos = [ maxima[0]-2*period0, maxima[0]-period0, maxima, $
        maxima[nmaxima-1]+period1, maxima[nmaxima-1]+2*period1 ]
    maxval = [ x[maxima[0]], x[maxima[0]], x[maxima], x[maxima[nmaxima-1]], $
        x[maxima[nmaxima-1]] ]
    minpos = [ minima[0]-2*period0, minima[0]-period0, minima, $
        minima[nminima-1]+period1, minima[nminima-1]+2*period1 ]
    minval = [ x[minima[0]], x[minima[0]], x[minima], x[minima[nminima-1]], $
        x[minima[nminima-1]] ]

    ; Estimate local mean.
    ; If we want to take the spline of the means of the extrema
    if splinemeanopt then begin

      ; Initialise position and value of individual local mean estimates
      meanpos = [ 0 ]
      meanval = [ 0 ]
      ; If the first extremum is a minimum, do it first
      if minpos[0] lt maxpos[0] then begin
        meanpos = [ meanpos, ( minpos[0] + maxpos[0] ) / 2 ]
        meanval = [ meanval, ( minval[0] + maxval[0] ) / 2. ]
      endif
      ; Now iterate through all maxima, taking the average of this maximum and
      ; the following minimum, and the following minimum and following maximum.
      for i = 0 * idtype, nmaxima + 4 - 1 do begin
        ; Determine the position of the next minimum after this maximum
        id1 = min( where( minpos gt maxpos[i] ), nid )
        ; If such a minimum exists
        if nid ne 0 then begin
          ; Add the average position and value to our collection
          meanpos = [ meanpos, ( maxpos[i] + minpos[id1] ) / 2 ]
          meanval = [ meanval, ( maxval[i] + minval[id1] ) / 2. ]
          ; Determine the position of the next maxmum after this minimum
          id2 = min( where( maxpos gt minpos[id1] ), nid )
          ; If such a maximum exists
          if nid ne 0 then begin
            ; Add the average position and value to our collection
            meanpos = [ meanpos, ( maxpos[id2] + minpos[id1] ) / 2 ]
            meanval = [ meanval, ( maxval[id2] + minval[id1] ) / 2. ]
          endif
        endif
      endfor
      ; Measure the number of estimates we have
      nmean = n_elements( meanpos ) - 1
      ; Sort the estimates (not guaranteed by our method) and remove 
      ; initialising values
      id = sort( meanpos[1:nmean] )
      meanpos = meanpos[1+id]
      meanval = meanval[1+id]
      ; Estimate the local mean through a spline interpolation
      localmean = spline( meanpos, meanval, indgen( ndata ) )

    ; If we want to take the mean of the splines of the extrema
    endif else begin

      ; Spline interpolate to get maximum and minimum envelopes
      maxenv = spline( maxpos, maxval, indgen( ndata ) )
      minenv = spline( minpos, minval, indgen( ndata ) )
      ; Estimate the local mean as the mean of these envelopes
      localmean = ( minenv + maxenv ) / 2.

    endelse

    ; Substract local mean from current data
    xold = x
    x = x - localmean

    ; If the IMF criterion is the extrema/zero crossings comparison
    if zerocrossopt then begin
      ; Count the number of zero crossings
      nzeroes = zero_cross( x )
      ; Count the number of extrema
      nextrema = n_elements( extrema( x ) )
      ; Check if the number of zero crossings equals the number of extrema, 
      ; to within one.
      if nextrema - nzeroes le 1 then begin
        ; Count this as a candidate IMF
        checkimf = checkimf + 1
      endif else begin
        ; Do not count this as a candidate IMF
        checkimf = 0
      endelse
    endif

    ; If the IMF criterion is checking the size of the difference between 
    ; successive rounds.
    if not( zerocrossopt ) then begin
      ; Measure which will be used to stop the sifting process.  Huang refers 
      ; to this as the standard deviation (SD) even though it is not.

      ; Calculate SD the traditional way
      if not( quekopt ) then begin
        sd = total( ( ( xold - x )^2 ) / ( xold^2 + epsilon ) )
      ; Or Quek et alii's modified way
      endif else begin
        sd = total( ( xold - x ) ^ 2 ) / total( xold^2 )
      endelse

      ; Compare sd value against threshold
      if sd lt shiftfactor then begin
        ; Count this as a candidate IMF
        checkimf = checkimf + 1
      endif else begin
        ; Do not count this as a candidate IMF
        checkimf = 0
      endelse

    endif

    ; Check for very small residual.  Call it an IMF.
    if stddev( x ) lt epsilon * stddev( data ) then checkres = checkres + 1

    ; Check to see if we have a satisfied IMF
    if checkimf eq ncheckimf then check = checkimfval
    if checkres eq ncheckimf then check = checkimfval

  endwhile

  ; Store the extracted IMF in the matrix imf
  if var_type( imf ) eq 0 then begin
    ; If this is the first IMF
    imf = x
  endif else begin
    ; If this is a later IMF
    imf = [ [imf], [x] ]
  endelse
  ; If we have hit the residual, then exit loop (and function)
  if check eq checkresval then begin
    check = checkexitval
  endif else begin
    check = 0
  endelse

  ; Substract the extracted IMF from the signal
  x = x0 - x

endwhile

;***********************************************************************
; The End

return, imf
END
