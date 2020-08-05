;+
; NAME:
;    VAR_TYPE
;
; PURPOSE:
;    This function returns the IDL code of the variable type.
;
; CATEGORY:
;    Miscellaneous, Optimal Detection Package v3.1.1
;
; CALLING SEQUENCE:
;    Result = VAR_TYPE( INVAR )
;
; INPUTS:
;    INVAR:  The variable to have its type returned.
;
; KEYWORD PARAMETERS:
;    HELP:  If set the function prints the name of the variable type to 
;        screen.  Default is no printing.
;    TEXT:  If set the function returns a text string instead of a number.
;
; OUTPUTS:
;    Result:  The IDL code of the variable type.  See the HELP option section 
;        of the function for interpretation of the code, or use the HELP 
;        keyword.
;
; PROCEDURE:
;    This function reads the variable type index from the SIZE function.
;
; EXAMPLE:
;    Define a floating point number.
;      x = 1.2
;    Find out its variable type.
;      result = var_type( x, /help )
;
; MODIFICATION HISTORY:
;    Written by:  Edward Wiebe, 2000-01-21.
;    Modified:  Daithi Stone, 2000-06-29 (changed behaviour of HELP keyword).
;    Modified:  ECW, 2001-05-08 (added text keyword)
;    Modified:  DAS, 2011-11-06 (modified format;  inclusion in Optimal 
;        Detection Package category)
;-

;***********************************************************************

FUNCTION VAR_TYPE, $
    INVAR, $
    HELP=help_opt, $
    TEXT=text_opt

;***********************************************************************
; Determine variable type

; Determine variable type
siz = size( invar )
type = siz[ siz[0]+1 ]

; Build vector of names
if keyword_set( help_opt ) or keyword_set( text ) then begin
  names = [ 'Undefined', 'Byte', 'Integer', 'Longword integer', $
      'Floating point', 'Double-precision floating', 'Complex floating', $
      'String', 'Structure', 'Double-precision complex floating', $
      'Pointer', 'Object reference' ]
endif

; If HELP is set
if keyword_set( help_opt ) then print, names[type]

; If text output is requested
if keyword_set( text_opt ) then type = names[type]

;***********************************************************************
; The end

return, type
END
