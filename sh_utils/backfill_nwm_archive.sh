#!/bin/sh

# Use Google Cloud to backfill missing NWM archives.
# Analysis forcing version.

get_nwm_from_google() {
    REMOTE_PATH=$1
    LOCAL_PATH=$2
    URL=https://storage.googleapis.com/national-water-model/${REMOTE_PATH}
    # Check for the remote file.
    curl --silent --head "$URL" | \
        head -n 1 | grep "HTTP/1.[01] [23].." > /dev/null
    STATUS=$?
    if [ $STATUS -ne 0 ] ; then
        usr_msg "Remote file $REMOTE_PATH does not exist."
    else
        # Verify the local directory exists; create if needed.
        if [ ! -d $(dirname $LOCAL_PATH) ] ; then
            mkdir -pv $(dirname $LOCAL_PATH)
        fi
        # Get the remote file.
        if [ "$TTY" == "FALSE" ] ; then
            SILENT_OPTION="--silent"
            usr_msg "Getting $URL"
        else
            SILENT_OPTION=
        fi
        curl $SILENT_OPTION --fail \
            -o ${LOCAL_PATH}.part \
            --remote-time "$URL"
        STATUS=$?
        if [ $STATUS -ne 0 ] ; then
            err_msg "Error getting $URL"
            err_out
        fi
        mv -v ${LOCAL_PATH}.part ${LOCAL_PATH}
    fi
}

EMAIL=Gregory.Fall@noaa.gov
BUSY_FILE=BACKFILL_NWM_ARCHIVE_BUSY
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

# For any given cycle hour, we want the following:
# - Analysis "tm00" files
# - If the cycle hour is 16Z, Eetended analysis "tm04" to "tm27" files

START_CYCLE_DATE="2019-10-01 00:00:00"
CUTOFF_CYCLE_DATE=`date -u --date="-47 hours" +"%Y-%m-%d %H:00:00"`

CYCLE_DATE=$START_CYCLE_DATE

while [ "$CYCLE_DATE" != "$CUTOFF_CYCLE_DATE" ] ; do

    CYCLE_DATE_YYYYMMDD=${CYCLE_DATE:0:4}${CYCLE_DATE:5:2}${CYCLE_DATE:8:2}
    DAY_DIR=${DEST_DIR_ROOT}/${CYCLE_DATE:0:4}/${CYCLE_DATE:5:2}/nwm.${CYCLE_DATE_YYYYMMDD}
    # echo $DAY_DIR

    T1=`date -u --date="$CYCLE_DATE" +%s`
    T2=`date -u --date="$CUTOFF_CYCLE_DATE" +%s`
    LAG_SECONDS=$((T2-T1))
    LAG_DAYS=$((LAG_SECONDS/86400))

    # Check for "tm00" forcing_analysis_assim files.
    DIR=${DAY_DIR}/forcing_analysis_assim
    LOCAL_FILE=nwm.${CYCLE_DATE_YYYYMMDD}.t${CYCLE_DATE:11:2}z.analysis_assim.forcing.tm00.conus.nc
    REMOTE_PATH=nwm.${CYCLE_DATE_YYYYMMDD}/forcing_analysis_assim/nwm.t${CYCLE_DATE:11:2}z.analysis_assim.forcing.tm00.conus.nc
    if [ ! -f ${DIR}/${LOCAL_FILE} ] ; then
        get_nwm_from_google $REMOTE_PATH ${DIR}/${LOCAL_FILE}
    fi

    if [ $LAG_DAYS -lt 64 ] ; then
        # Check for "tm01" and "tm02" forcing_analysis_assim files.
        DIR=${DAY_DIR}/forcing_analysis_assim
        LOCAL_FILE=nwm.${CYCLE_DATE_YYYYMMDD}.t${CYCLE_DATE:11:2}z.analysis_assim.forcing.tm01.conus.nc
        REMOTE_PATH=nwm.${CYCLE_DATE_YYYYMMDD}/forcing_analysis_assim/nwm.t${CYCLE_DATE:11:2}z.analysis_assim.forcing.tm01.conus.nc
        if [ ! -f ${DIR}/${LOCAL_FILE} ] ; then
            get_nwm_from_google $REMOTE_PATH ${DIR}/${LOCAL_FILE}
        fi
        LOCAL_FILE=nwm.${CYCLE_DATE_YYYYMMDD}.t${CYCLE_DATE:11:2}z.analysis_assim.forcing.tm02.conus.nc
        REMOTE_PATH=nwm.${CYCLE_DATE_YYYYMMDD}/forcing_analysis_assim/nwm.t${CYCLE_DATE:11:2}z.analysis_assim.forcing.tm02.conus.nc
        if [ ! -f ${DIR}/${LOCAL_FILE} ] ; then
            get_nwm_from_google $REMOTE_PATH ${DIR}/${LOCAL_FILE}
        fi
    fi

    # Check for "tm00" analysis_assim (land) files.
    DIR=${DAY_DIR}/analysis_assim
    LOCAL_FILE=nwm.${CYCLE_DATE_YYYYMMDD}.t${CYCLE_DATE:11:2}z.analysis_assim.land.tm00.conus.nc
    REMOTE_PATH=nwm.${CYCLE_DATE_YYYYMMDD}/analysis_assim/nwm.t${CYCLE_DATE:11:2}z.analysis_assim.land.tm00.conus.nc
    if [ ! -f ${DIR}/${LOCAL_FILE} ] ; then
        get_nwm_from_google $REMOTE_PATH ${DIR}/${LOCAL_FILE}
    fi

    if [ $LAG_DAYS -lt 64 ] ; then
        # Check for "tm01" and "tm02" analysis_assim files.
        DIR=${DAY_DIR}/analysis_assim
        LOCAL_FILE=nwm.${CYCLE_DATE_YYYYMMDD}.t${CYCLE_DATE:11:2}z.analysis_assim.land.tm01.conus.nc
        REMOTE_PATH=nwm.${CYCLE_DATE_YYYYMMDD}/analysis_assim/nwm.t${CYCLE_DATE:11:2}z.analysis_assim.land.tm01.conus.nc
        if [ ! -f ${DIR}/${LOCAL_FILE} ] ; then
            get_nwm_from_google $REMOTE_PATH ${DIR}/${LOCAL_FILE}
        fi
        LOCAL_FILE=nwm.${CYCLE_DATE_YYYYMMDD}.t${CYCLE_DATE:11:2}z.analysis_assim.land.tm02.conus.nc
        REMOTE_PATH=nwm.${CYCLE_DATE_YYYYMMDD}/analysis_assim/nwm.t${CYCLE_DATE:11:2}z.analysis_assim.land.tm02.conus.nc
        if [ ! -f ${DIR}/${LOCAL_FILE} ] ; then
            get_nwm_from_google $REMOTE_PATH ${DIR}/${LOCAL_FILE}
        fi
    fi

    if [ "${CYCLE_DATE:11:2}" == "16" ] ; then
        # Check for forcing_analysis_assim_extended files.        
        DIR=${DAY_DIR}/forcing_analysis_assim_extend
        for tm in {27..0..-1} ; do
            if [ $tm -eq 3 ] || \
               [ $tm -eq 2 ] || \
               [ $tm -eq 1 ] ; then
                continue
            fi
            tm_str=`printf "%02d" $tm`
            LOCAL_FILE=nwm.${CYCLE_DATE_YYYYMMDD}.t${CYCLE_DATE:11:2}z.analysis_assim_extend.forcing.tm${tm_str}.conus.nc
            REMOTE_PATH=nwm.${CYCLE_DATE_YYYYMMDD}/forcing_analysis_assim_extend/nwm.t${CYCLE_DATE:11:2}z.analysis_assim_extend.forcing.tm${tm_str}.conus.nc
            if [ ! -f ${DIR}/${LOCAL_FILE} ] ; then
                get_nwm_from_google $REMOTE_PATH ${DIR}/${LOCAL_FILE}
            #     echo "DID IT WORK? ${DIR}/${LOCAL_FILE}"
            #     exit 1
            # else
            #     echo "WE HAVE ${DIR}/${LOCAL_FILE}"
            fi
        done

        # Check for analysis_assim_extended files.        
        DIR=${DAY_DIR}/analysis_assim_extend
        for tm in {27..0..-1} ; do
            if [ $tm -eq 3 ] || \
               [ $tm -eq 2 ] || \
               [ $tm -eq 1 ] ; then
                continue
            fi
            tm_str=`printf "%02d" $tm`
            LOCAL_FILE=nwm.${CYCLE_DATE_YYYYMMDD}.t${CYCLE_DATE:11:2}z.analysis_assim_extend.land.tm${tm_str}.conus.nc
            REMOTE_PATH=nwm.${CYCLE_DATE_YYYYMMDD}/analysis_assim_extend/nwm.t${CYCLE_DATE:11:2}z.analysis_assim_extend.land.tm${tm_str}.conus.nc
            if [ ! -f ${DIR}/${LOCAL_FILE} ] ; then
                get_nwm_from_google $REMOTE_PATH ${DIR}/${LOCAL_FILE}
            #     echo "DID IT WORK? ${DIR}/${LOCAL_FILE}"
            #     exit 1
            # else
            #     echo "WE HAVE ${DIR}/${LOCAL_FILE}"
            fi
        done
    fi






    CYCLE_DATE=`date -u --date="+1 hour $CYCLE_DATE" +"%Y-%m-%d %H:00:00"`

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



