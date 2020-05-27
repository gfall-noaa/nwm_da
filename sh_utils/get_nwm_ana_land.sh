#!/bin/sh

# Fetch National Water Model analysis/assim land model data.

# Greg Fall, 2019-10-01 - adapted from get_nwm_ana_ext_land.sh

# Example https and ftp locations:
# https://para.nomads.ncep.noaa.gov/pub/data/nccf/com/nwm/para/nwm.20190220/forcing_analysis_assim/
# https://para.nomads.ncep.noaa.gov/pub/data/nccf/com/nwm/para/nwm.20190220/analysis_assim/
# ftp://ftp.ncep.noaa.gov/pub/data/nccf/com/para/com/nwm/para/nwm.20190220/forcing_analysis_assim/
# ftp://ftp.ncep.noaa.gov/pub/data/nccf/com/para/com/nwm/para/nwm.20190220/analysis_assim/

EMAIL=Gregory.Fall@noaa.gov
BUSY_FILE=ARCHIVE_NWM_ANA_BUSY
MAX_INSTANCES=1

# Source common functions.

SH_UTILS_DIR=/nwcdev/sh_utils
if [ ! -d "$SH_UTILS_DIR" ] ; then
  echo "Bad configuration - missing $SH_UTILS_DIR" \
       "directory" 1>&2
  exit 1
fi
if [ ! -f ${SH_UTILS_DIR}/cron_script_functions.sh ] ; then
    echo "Missing ${SH_UTILS_DIR}/cron_script_functions.sh" 1>&2
    exit 1
fi
. ${SH_UTILS_DIR}/cron_script_functions.sh

# Source key process/log management "start" code.

INCLUDE_PATH=${SH_UTILS_DIR}/nwcdev_script_top.sh
if [ ! -f "$INCLUDE_PATH" ] ; then
    err_msg "Missing ${INCLUDE_PATH} file"
    err_out
fi
. $INCLUDE_PATH


#####################
# MAIN SCRIPT BEGIN #
#####################

DATE_YYYY=`echo ${DATE_YYYYMMDDHHMMSS:0:4}`
DATE_MM=`echo ${DATE_YYYYMMDDHHMMSS:4:2}`
DATE_DD=`echo ${DATE_YYYYMMDDHHMMSS:6:2}`


# Make sure necessary scripts and utilities are present.

which curl > /dev/null
STATUS=$?
if [ $STATUS -ne 0 ] ; then
  err_msg "No curl executable in path."
  err_out
fi

# Check destination directory.

DEST_DIR_ROOT=/nwcdev/archive/NWM_v2.0_archive
if [ ! -d $DEST_DIR_ROOT ] ; then
  err_msg "Destination directory ${DEST_DIR_ROOT} not found."
  err_out
else
  if [ ! -w $DEST_DIR_ROOT ] ; then
    err_msg "Destination directory ${DEST_DIR_ROOT} not writable for user" \
            "${USER}."
    err_out
  fi
fi

REMOTE_HOST="nomads.ncep.noaa.gov"
REMOTE_DIR="pub/data/nccf/com/nwm/prod"

HOURS_BACK=72

while [ $HOURS_BACK -ge 0 ] ; do

    # Calculate model cycle.
    CYCLE_DATE_DOY=`date -u --date="-${HOURS_BACK} hours ${DATE}" +%j`
    CYCLE_DATE_YYYYMMDDHH=`date -u --date="-${HOURS_BACK} hours ${DATE}" \
                           +%Y%m%d%H`
    CYCLE_DATE_YYYYMMDD=${CYCLE_DATE_YYYYMMDDHH:0:8}
    CYCLE_DATE_YYYY=${CYCLE_DATE_YYYYMMDDHH:0:4}
    CYCLE_DATE_MM=${CYCLE_DATE_YYYYMMDDHH:4:2}
    CYCLE_DATE_DD=${CYCLE_DATE_YYYYMMDDHH:6:2}
    CYCLE_DATE_HH=${CYCLE_DATE_YYYYMMDDHH:8:2}

    usr_msg "Checking for $CYCLE_DATE_YYYYMMDDHH"

    # Check for source directory on remote host.
    URL="https://${REMOTE_HOST}/${REMOTE_DIR}/nwm.${CYCLE_DATE_YYYYMMDD}"
    curl --silent --fail -o /dev/null "$URL"
    STATUS=$?
    if [ $STATUS -ne 0 ] ; then
        if [ $STATUS -ne 22 ] ; then
            err_msg "Unknown curl error for $URL"
            err_out
        fi
        # Source directory is not there.
        usr_msg "Directory nwm.${CYCLE_DATE_YYYYMMDD} not present on server."
        HOURS_BACK=$((HOURS_BACK-1))
        continue
    fi
    usr_msg "Remote directory $URL found."

    # Check for local destination directory. Create if necessary.
    DEST_DIR_PARENT=${DEST_DIR_ROOT}/${CYCLE_DATE_YYYY}/${CYCLE_DATE_MM}/nwm.${CYCLE_DATE_YYYYMMDD}
    if [ ! -d $DEST_DIR_PARENT ] ; then
        mkdir -p -m 2775 $DEST_DIR_PARENT
        STATUS=$?
        if [ $STATUS -ne 0 ] ; then
            err_msg "FATAL: Failed to create destination directory" \
                    "${DEST_DIR_PARENT}."
            err_out
        fi
    else
        if [ ! -w $DEST_DIR_PARENT ] ; then
            err_msg "Destination directory ${DEST_DIR_PARENT} not writable" \
                    "for user ${USER}."
            err_out
        fi
    fi

    # Verify that the "analysis_assim_extend" subdirectory is
    # present on the server.
    REMOTE_DIR_URL="${URL}/analysis_assim"
    curl --silent --fail -o /dev/null "$REMOTE_DIR_URL"
    STATUS=$?
    if [ $STATUS -ne 0 ] ; then
        if [ $STATUS -ne 22 ] ; then
            err_msg "Unknown curl error for $REMOTE_DIR_URL"
            err_out
        fi
        usr_msg "Remote directory $REMOTE_DIR_URL does not exist."
    else

        # Check for local analysis_assim_extend directory.
        DEST_DIR_CHILD=${DEST_DIR_PARENT}/analysis_assim
        if [ ! -d $DEST_DIR_CHILD ] ; then
            mkdir -m 2775 $DEST_DIR_CHILD
            STATUS=$?
            if [ $STATUS -ne 0 ] ; then
                err_msg "FATAL: Failed to create destination directory" \
                        "${DEST_DIR_CHILD}."
                err_out
            fi
        else
            if [ ! -w $DEST_DIR_CHILD ] ; then
                err_msg "Destination directory ${DEST_DIR_CHILD}"
                        "not writable for user ${USER}."
                err_out
            fi
        fi

        # Loop over "time minus" hours. 
        TIME_MINUS=2
        while [ $TIME_MINUS -ge 0 ] ; do

            TIME_MINUS_STR=`printf "%02d" $TIME_MINUS`

            # Check for the local file.
            LOCAL_FILE="nwm.${CYCLE_DATE_YYYYMMDD}.t${CYCLE_DATE_HH}z.analysis_assim.land.tm${TIME_MINUS_STR}.conus.nc"

            if [ -f ${DEST_DIR_CHILD}/${LOCAL_FILE} ] && \
               [ -n ${DEST_DIR_CHILD}/${LOCAL_FILE} ] ; then
                usr_msg "Already have ${DEST_DIR_CHILD}/${LOCAL_FILE}"
                TIME_MINUS=$((TIME_MINUS-1))
                continue
            fi

            # Check for the remote file.
            REMOTE_FILE="nwm.t${CYCLE_DATE_HH}z.analysis_assim.land.tm${TIME_MINUS_STR}.conus.nc"
            REMOTE_FILE_URL="${REMOTE_DIR_URL}/${REMOTE_FILE}"
            curl --silent --head "$REMOTE_FILE_URL" | \
              head -n 1 | grep "HTTP/1.[01] [23].." > /dev/null
            STATUS=$?
            if [ $STATUS -ne 0 ] ; then
                usr_msg "Remote file $REMOTE_FILE_URL does not exist."
                TIME_MINUS=$((TIME_MINUS-1))
                continue
            fi

            # Fetch the remote file.
            if [ "$TTY" == "FALSE" ] ; then
                SILENT_OPTION="--silent"
                usr_msg "Getting $REMOTE_FILE_URL"
            else
                SILENT_OPTION=
            fi
            curl $SILENT_OPTION --fail \
                -o ${DEST_DIR_CHILD}/${LOCAL_FILE}.part \
                --remote-time "$REMOTE_FILE_URL"
            STATUS=$?
            if [ $STATUS -ne 0 ] ; then
               err_msg "Error getting $REMOTE_FILE_URL"
               err_out
            fi
	    ## Get the modification time.
            #LINE=`curl $SILENT_OPTION --head "$REMOTE_FILE_URL" | \
            #      grep "Last.Modified:"`
            #STATUS=$?
            #if [ $STATUS -ne 0 ] ; then
            #    err_msg "WARNING: failed to get modification time for" \
            #            "$REMOTE_FILE_URL"
            #else
            #    # Apply the modification time.
            #    MOD_TIME=`echo $LINE | sed 's/Last.Modified:\ *//g'`
            #    MOD_TIME_TOUCH=`date --date="$MOD_TIME" "+%Y%m%d%H%M.%S"`
            #    touch -t $MOD_TIME_TOUCH ${DEST_DIR_CHILD}/${LOCAL_FILE}.part
            #fi
            mv -v ${DEST_DIR_CHILD}/${LOCAL_FILE}.part \
                  ${DEST_DIR_CHILD}/${LOCAL_FILE}

            TIME_MINUS=$((TIME_MINUS-1))

        done

    fi

    HOURS_BACK=$((HOURS_BACK-1))

done

###################
# MAIN SCRIPT END #
###################


# Source key process/log management "finish" code and exit.

INCLUDE_PATH=${SH_UTILS_DIR}/nwcdev_script_bottom.sh
if [ ! -f "$INCLUDE_PATH" ] ; then
    err_msg "Missing ${INCLUDE_PATH} file"
    err_out
fi
. $INCLUDE_PATH


