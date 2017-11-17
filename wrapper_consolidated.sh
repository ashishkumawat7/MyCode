####### wrapper hive ########## ashish #######
#!/bin/ksh
#########################################################
# Wrapper Script for Hive Jobs
# 
# Zeke will invoke this script by passing Zeke Job Name
# as a parameter.
# 
# 
# 
# 
#
# Revision History
# 0.1 10/06/2015 Initial draft.
#
#########################################################
#set -x
ZEKE_JOB_NAME=$1
ROOT_PATH='/u01/datascience/common/bin'
. $ROOT_PATH/functions/commonutil.ksh
. $ROOT_PATH/functions/hiveutil.ksh
TMP='/tmp'
ID=$RANDOM
user_id=$(whoami)
server_name=$(hostname)
ENV_FILE="$ROOT_PATH/common.env"
echo "ZEKE_JOB_NAME :" $ZEKE_JOB_NAME
echo "ENV_FILE : " $ENV_FILE 
if [[ -z $ZEKE_JOB_NAME ]]; then
  echo "Error: Missing zeke-job-nam parameter."
  echo "USAGE: wrapper_ds.ksh zeke-job-name "
  exit 1
fi
source $ENV_FILE
if [ $? -ne 0 ]; then
  echo "Error: Missing env file."
  exit 1
fi
sid=$user_id
echo "sid:" $sid
usid=$(echo $sid | tr '[:lower:]' '[:upper:]')
echo "usid : " $usid
/usr/bin/kinit $usid@AETH.AETNA.COM -k -t /users/$sid/$sid.keytab
date=`date -d"-1 days" +%Y-%m-%d`
yr=`echo $date | awk -F\- '{print $1}'`
mth=`echo $date | awk -F\- '{print $2}'`
dy=`echo $date | awk -F\- '{print $3}'`
PROGRESS="InProgress"
COMPLETE="Complete"
START_TIME=$(date  +%Y-%m-%d_%T)
START_TIME_S=$SECONDS
PARAM_LOG_FILE=""$TMP"/"$ZEKE_JOB_NAME-$START_TIME-$ID-PARAMS".log"
echo "Log file : " $PARAM_LOG_FILE
PARAM_FILE=""$TMP"/"$ZEKE_JOB_NAME-$START_TIME-$ID-PARAM".txt"
echo "Param file : " $PARAM_FILE
ERRORLIST_FILE=""$TMP"/"$ZEKE_JOB_NAME-$START_TIME-$ID-ERRORLIST".txt"
typeset -A error_msgs
##########  Get the job attributes from the Job Master table ############
SQLSTMT="select j.job_name, a.email , a.script_path , a.log_path , COALESCE(nullif(j.job_path,''),'-') , j.tools_used , j.retry_attempt ,coalesce(nullif(j.op_stats_table,''),'NULL') , a.app_name , j.job_id , a.yarn_logs ,a.app_id, j.batchid, j.batchname  from $APP_TABLE as a join $JOB_TABLE as j on j.app_id=a.app_id  where j.Zeke_Job_Name='$ZEKE_JOB_NAME';"
echo "SQL statement: " $SQLSTMT
execute_sql "$SQLSTMT" SQLRESULT RC
  echo "RC: " $RC
  if [ $RC -ne 0 ]; then
    echo "inside" 
    EMAIL_REPORT=""$TMP"/temp.txt"
    EMAIL_TO=$EMAIL_ADDR
    echo "Job $ZEKE_JOB_NAME failed to load parameters."
    echo -e "Message: Failed to load parameters from app & job tables." >> $EMAIL_REPORT
    report_job_run "Failed "
    echo "EMAIL_REPORT :" $EMAIL_REPORT
    echo "EMIL_TO : " $EMAIL_TO
    if [[  -e $EMAIL_REPORT ]]; then
     rm $EMAIL_REPORT
    fi
    exit 1 
  fi
echo $SQLRESULT | read HIVE_JOB_NAME EMAIL_TO SCRIPT_PATH LOG_PATH JOB_PATH TYPE RETRY STATS_TABLE APP_NAME JOB_ID YARN_LOGS_LOC APP_ID BATCH_ID BATCH_NAME
echo JOB_NAME is $HIVE_JOB_NAME
echo EMAIL_TO is $EMAIL_TO
echo SCRIPT_PATH is $SCRIPT_PATH
echo LOG_PATH is $LOG_PATH
echo JOB_PATH is $JOB_PATH
echo TYPE is $TYPE
echo RETRY is  $RETRY
echo STATS_TABLE is $STATS_TABLE
echo APP_NAME is $APP_NAME
echo YARN_LOGS_LOC is $YARN_LOGS_LOC
echo APP_ID is $APP_ID
echo BATCH_ID is $BATCH_ID
echo BATCH_NAME is $BATCH_NAME
##########  Get the cycle  attributes from the Job Cycle table ############
SQLSTMT="select status,cycle_id,app_id,batchid from $CYCLE_TABLE where app_id='$APP_ID'and batchid='$BATCH_ID' and status='InProgress' order by cycle_id desc limit 1 ;"
echo SQLSTMT is $SQLSTMT
execute_sql "$SQLSTMT" SQLRESULT RC
  echo "RC: " $RC
  if [ $RC -ne 0 ]; then
    echo "Error retrieving status: "
  fi 
echo $SQLRESULT | read STATUS CYCLE_ID APP_ID B_ID
echo STATUS is $STATUS
echo CYCLE_ID is $CYCLE_ID
echo APP_ID is $APP_ID
echo CYCLE BATCH_ID is $B_ID
if [[ "$STATUS" != $PROGRESS ]]; then
	echo Error: Cycle is not in progress, Please open the cycle and rerun !
        exit 1
fi
#########  Get  error list from the error  Master table ############
SQLSTMT="select script_type, error from $ERROR_TABLE where script_type='hive'";
echo "SQL statement: " $SQLSTMT
SQLRESULT=`mysql --skip-column-names -h$HOST --port $PORT  -u$USER -p$PW -e "$SQLSTMT"`
RC=$?
echo "RC: " $RC
if [ $RC -ne 0 ]; then
    echo "inside"
    EMAIL_REPORT=""$TMP"/temp.txt"
    EMAIL_TO=$EMAIL_ADDR
    echo "Job $ZEKE_JOB_NAME failed to load error list."
    echo -e "Message: Failed to load parameters from error table." >> $EMAIL_REPORT
    report_job_run "Failed "
    echo "EMAIL_REPORT :" $EMAIL_REPORT
    echo "EMIL_TO : " $EMAIL_TO
    if [[  -e $EMAIL_REPORT ]]; then
     rm $EMAIL_REPORT
    fi
    exit 1
fi
echo "$SQLRESULT" >> $ERRORLIST_FILE
k=0
######### Construct error list #########
cat $ERRORLIST_FILE | while read line
 do
  k=`expr $k + 1`
  script_type=`echo $line | cut -f1 -d' '`
  error=`echo $line | cut -f2- -d' '`
  if [[ "$script_type" == 'hive' ]]; then
        echo script type is hive....
        error_msgs[$k]=$error
  fi
done
#########  Get the param  attributes from the Job Param table ############
SQLSTMT="select p.param_name,p.param_value from $PARAM_TABLE as p join $JOB_TABLE as j on p.job_id=j.job_id where j.Zeke_Job_Name='$ZEKE_JOB_NAME';"
echo "SQL statement: " $SQLSTMT
SQLRESULT=`mysql --skip-column-names -h$HOST --port $PORT  -u$USER -p$PW -e "$SQLSTMT"`
RC=$?
echo "RC: " $RC
if [ $RC -ne 0 ]; then
    echo "inside" 
    EMAIL_REPORT=""$TMP"/temp.txt"
    EMAIL_TO=$EMAIL_ADDR
    echo "Job $ZEKE_JOB_NAME failed to load parameters."
    echo -e "Message: Failed to load parameters from param table." >> $EMAIL_REPORT
    report_job_run "Failed "
    echo "EMAIL_REPORT :" $EMAIL_REPORT
    echo "EMIL_TO : " $EMAIL_TO
    if [[  -e $EMAIL_REPORT ]]; then
     rm $EMAIL_REPORT
    fi
    exit 1 
  fi
echo "$SQLRESULT" >> $PARAM_FILE
######### Construct parameter list #########
cat $PARAM_FILE | while read line
 do
  echo "line:" $line
  F1=`echo $line | cut -f1 -d' '`
  F2=`echo $line | cut -f2- -d' '`
  
  if [[ ! -z $F1 && ! -z $F2 ]]; then
     C1=`echo $F2 | cut -c1-1`
      if [[ "$C1" == '`' ]]; then
         eval F2VAL=$F2
          echo "F2VAL : " $F2VAL
         PARAMLIST=$(echo $PARAMLIST --hiveconf $F1=$F2VAL)
      else
         PARAMLIST=$(echo $PARAMLIST --hiveconf $F1=$F2)
    fi
  fi
 done 
 echo PARAMLIST is $PARAMLIST
JOB_TYPE=`echo $HIVE_JOB_NAME | cut -f2- -d'.'`
if [ "$DEPLOY_MODE" == "AUTO" ]; then
   SCRIPTS=""
   LOGS=""
else
   SCRIPTS="/scripts"
   LOGS="/logs"
fi
if [ "$JOB_PATH" == "-" ]; then
   SCRIPT_DIR=${SCRIPT_PATH}
   LOG_DIR=${LOG_PATH}
else
   SCRIPT_DIR=${SCRIPT_PATH}/${JOB_PATH}${SCRIPTS}
   LOG_DIR=${LOG_PATH}/${JOB_PATH}${LOGS}  
fi    
if [ ! -d "$LOG_DIR" ]; then
   mkdir -p $LOG_DIR
   chmod -R 775 $LOG_DIR
fi 
if [[ "$TYPE" == "hive" || "$TYPE" == "beeline" ]]; then
     if [ "$JOB_TYPE" == "hql" ]; then
         HQL_FILE=${SCRIPT_DIR}/${HIVE_JOB_NAME}
     else
         HQL_FILE=${SCRIPT_DIR}/${HIVE_JOB_NAME}.hql
     fi
else
     echo "Exit: not hive job !" 
     exit 1
fi
echo "HQL File Name: " $HQL_FILE
echo "Job_Name: " $HIVE_JOB_NAME
echo "Param List :" $PARAMLIST
######## Set the log directory path and the log file name. ##########
#if [ "$JOB_PATH" == "-" ]
#then
#LOG_FILE=""$LOG_PATH"/"$HIVE_JOB_NAME-$START_TIME-$ID".log"
#else
#LOG_FILE=""$LOG_PATH"/"$JOB_PATH""${LOGS}""$HIVE_JOB_NAME-$START_TIME-$ID".log"
#fi
LOG_FILE=""$LOG_DIR"/"$HIVE_JOB_NAME-$START_TIME-$ID".log"
echo "Log file : " $LOG_FILE
EMAIL_REPORT=""$TMP"/"$HIVE_JOB_NAME-$START_TIME"-email-report-$ID.txt"
echo "Email report file: " $EMAIL_REPORT
DETAIL_REPORT=""$TMP"/"$HIVE_JOB_NAME"-detail-report-"$ID".txt"
HIVE_INPUT=""$HIVE_STANDARD_PARAMS" "$PARAMLIST" -f "$HQL_FILE""
echo "Hive String: " $HIVE_INPUT
job_start_time=$(date +%Y-%m-%d_%T)
 if [[ "$TYPE" == "beeline" ]]; then
      echo "Beeline: Starting"
      HIVE_INPUT=$(echo $HIVE_INPUT | sed -e "s/\hiveconf/hivevar/g")
      echo $HIVE_INPUT
      echo $BEELINE
      $BEELINE $HIVE_INPUT >> $LOG_FILE 2>&1
      RC=$?
  else
      $HIVE $HIVE_INPUT >> $LOG_FILE 2>&1
      RC=$?
fi
echo HIVE RC: $RC
#if [ $RC -ne 0 ]; then
#       echo "TOTAL RETRIES: " $RETRY
#       i=0
#       while [ $RETRY -gt i ]
#       do
#	 echo "Retry # : " $i 
#	 $HIVE $HIVE_INPUT >> $LOG_FILE-$i 2>&1
#	 RC=$?
#	 if [ $RC -eq 0 ]; then
#	   break
#	 fi
#	 let i=i+1
#       done
#fi
j=0
while [ $RETRY -gt j ]
 do
	for index in "${!error_msgs[@]}"; do
		error=${error_msgs["$index"]}
		#echo error : $error
		if [ $(cat $LOG_FILE | grep -i "$error" | wc -w) -gt 0 ]; then
			 echo Log file contains error $error
			 #echo "Retry # : " $j
			 LOG_FILE=$LOG_FILE-RESTART-$j
			 echo "Hive String: " $HIVE_INPUT
			 $HIVE $HIVE_INPUT >> $LOG_FILE  2>&1
			 RC=$?
			 break
		else
			echo Log file does not contain error : $error
		fi
	done
	let j=j+1
done
echo RC is  $RC
ZEKE_RC=$RC
echo LOG_FILE is $LOG_FILE
extract_hive_log $APP_ID $ZEKE_JOB_NAME $START_TIME $ID $RC $TMP
if [[  -e $ERRORLIST_FILE ]]; then
 rm $ERRORLIST_FILE
fi
if [[  -e $DETAIL_REPORT ]]; then
   rm $DETAIL_REPORT
fi
if [[  -e $PARAM_FILE ]]; then
   rm $PARAM_FILE
fi
if [[  -e $PARAM_LOG_FILE ]]; then
   rm $PARAM_LOG_FILE
fi
if [[  -e $YARN_LOG_FILE ]]; then
   rm $YARN_LOG_FILE 
fi
if [[  -e $EMAIL_REPORT ]]; then
 rm $EMAIL_REPORT
fi
if [[ $ZEKE_RC -ne 0 ]]; then
   echo " ZEKE Exit RC : " $ZEKE_RC
         exit 1
fi



######################################## wrapper pig ##########################################

#!/bin/ksh
#########################################################
# Wrapper Script for PIG Jobs
#
# Zeke will invoke this script by passing Zeke Job Name
# as a parameter.
#
#
#
#
#
# 
#
#########################################################
#set -x

ZEKE_JOB_NAME=$1
ROOT_PATH='/u01/datascience/common/bin'
TMP='/tmp'

ID=$RANDOM

user_id=$(whoami)
server_name=$(hostname)

ENV_FILE="$ROOT_PATH/common.env"
echo "ZEKE_JOB_NAME :" $ZEKE_JOB_NAME
echo "ENV_FILE : " $ENV_FILE 
echo $ROOT_PATH

if [[ -z $ZEKE_JOB_NAME ]]; then
  echo "Error: Missing zeke-job-nam parameter."
  echo "USAGE: wrapper_ds.ksh zeke-job-name "
  exit 1
fi

#sourcing lib functions
. /u01/datascience/common/bin/functions/pigutil.ksh
source $ENV_FILE
if [ $? -ne 0 ]; then
  echo "Error: Missing env file."
  exit 1
fi


sid=$user_id
echo "sid:" $sid
usid=$(echo $sid | tr '[:lower:]' '[:upper:]')
echo "usid : " $usid


/usr/bin/kinit $usid@AETH.AETNA.COM -k -t /users/$sid/$sid.keytab


date=`date -d"-1 days" +%Y-%m-%d`
yr=`echo $date | awk -F\- '{print $1}'`
mth=`echo $date | awk -F\- '{print $2}'`
dy=`echo $date | awk -F\- '{print $3}'`

PROGRESS="InProgress"
COMPLETE="Complete"

START_TIME=$(date  +%Y-%m-%d_%T)
echo "Start time: " $START_TIME

START_TIME_S=$SECONDS

PARAM_LOG_FILE=""$TMP"/"$ZEKE_JOB_NAME-$START_TIME-$ID-PARAMS".log"
echo "Log file : " $PARAM_LOG_FILE

PARAM_FILE=""$TMP"/"$PIG_JOB_NAME-$START_TIME-$ID-PARAM".txt"
echo "Param file : " $PARAM_FILE


##########  Get the job attributes from the Job Master table ############

SQLSTMT="select j.job_name as pig_job_name, a.email as email_to, a.script_path as script_path, a.log_path as log_path, COALESCE(nullif(j.job_path,''),'-') ,j.tools_used as type, j.retry_attempt as retry,coalesce(nullif(j.op_stats_table,''),'NULL') as stats_table, a.app_name as app_name, j.job_id as job_id, a.yarn_logs as yarn_logs_loc,a.app_id as app_id,j.batchid, j.batchname  from $APP_TABLE as a join $JOB_TABLE as j on j.app_id=a.app_id  where j.Zeke_Job_Name='$ZEKE_JOB_NAME';"
echo "SQL statement: " $SQLSTMT
execute_sql "$SQLSTMT" SQLRESULT RC

 echo "RC: " $RC
  if [ $RC -ne 0 ]; then
    echo "inside"
    EMAIL_REPORT=""$TMP"/temp.txt"
    EMAIL_TO=$EMAIL_ADDR

    echo "Job $ZEKE_JOB_NAME failed to load parameters."
    echo -e "Message: Failed to load parameters from app & job tables." >> $EMAIL_REPORT
    report_job_run "Failed "

    echo "EMAIL_REPORT :" $EMAIL_REPORT
    echo "EMIL_TO : " $EMAIL_TO

    if [[  -e $EMAIL_REPORT ]]; then
     rm $EMAIL_REPORT
   fi

   exit 1
  fi


echo $SQLRESULT | read PIG_JOB_NAME EMAIL_TO SCRIPT_PATH LOG_PATH JOB_PATH TYPE RETRY STATS_TABLE APP_NAME JOB_ID YARN_LOGS_LOC APP_ID BATCH_ID BATCH_NAME
echo JOB_NAME is $PIG_JOB_NAME
echo EMAIL_TO is $EMAIL_TO
echo SCRIPT_PATH is $SCRIPT_PATH
echo LOG_PATH is $LOG_PATH
echo JOB_PATH is $JOB_PATH
echo TYPE is $TYPE
echo RETRY is  $RETRY
echo STATS_TABLE is $STATS_TABLE
echo APP_NAME is $APP_NAME
echo JOB_ID is $JOB_ID
echo YARN_LOGS_LOC is $YARN_LOGS_LOC
echo APP_ID is $APP_ID
echo BATCH_ID is $BATCH_ID
echo BATCH_NAME is $BATCH_NAME
echo PW is $PW | sed "s/$PW/*******/"

##########  Get the cycle  attributes from the Job Cycle table ############
SQLSTMT="select status,cycle_id,app_id,batchid from $CYCLE_TABLE where app_id='$APP_ID'and batchid='$BATCH_ID' and status='InProgress' order by cycle_id desc limit 1 ;"
execute_sql "$SQLSTMT" SQLRESULT RC
  echo "RC: " $RC
  if [ $RC -ne 0 ]; then
    echo "Eror retrieving status: "
  fi 

echo $SQLRESULT | read STATUS CYCLE_ID APP_ID B_ID
echo STATUS is $STATUS
echo CYCLE_ID is $CYCLE_ID
echo APP_ID is $APP_ID
echo CYCLE BATCH_ID is $B_ID
if [[ "$STATUS" != $PROGRESS ]]; then
	echo Error: Please open the cycle and rerun !
        exit 1
fi

##########  Get the param  attributes from the Job Param table ############
SQLSTMT="select p.param_name,p.param_value from $PARAM_TABLE as p join $JOB_TABLE as j on p.job_id=j.job_id where j.Zeke_Job_Name='$ZEKE_JOB_NAME';"
echo "SQL statement: " $SQLSTMT
SQLRESULT=`mysql --skip-column-names -h$HOST --port $PORT  -u$USER -p$PW -e "$SQLSTMT"`
RC=$?
echo "RC: " $RC
if [ $RC -ne 0 ]; then
   echo "inside"
   EMAIL_REPORT=""$TMP"/temp.txt"
   EMAIL_TO=$EMAIL_ADDR

    echo "Job $ZEKE_JOB_NAME failed to load parameters."
    echo -e "Message: Failed to load parameters from param table." >> $EMAIL_REPORT
    report_job_run "Failed "

    echo "EMAIL_REPORT :" $EMAIL_REPORT
    echo "EMIL_TO : " $EMAIL_TO

    if [[  -e $EMAIL_REPORT ]]; then
        rm $EMAIL_REPORT
    fi

    exit 1
  fi


echo "$SQLRESULT" >> $PARAM_FILE

######### Construct parameter list #########
 cat $PARAM_FILE | while read line
  do
   echo "line:" $line 
  F1=`echo $line | cut -f1 -d' '`
  echo $F1
  F2=`echo $line | cut -f2- -d' '`
  echo $F2 
  if [[ ! -z $F1 && ! -z $F2 ]]; then
     C1=`echo $F2 | cut -c1-1`
      if [[ "$C1" == '`' ]]; then
              eval F2VAL=$F2
          echo "F2VAL : " $F2VAL
         PARAMLIST=$(echo $PARAMLIST -useHCatalog -param $F1=$F2VAL)
      else
      	 if [[ "$F1" =~ 'D' ]]; then
      	 			PARAMLIST=$(echo $F1=$F2 $PARAMLIST)
      	 else
         			PARAMLIST=$(echo $PARAMLIST -useHCatalog -param $F1=$F2)
         fi
    fi
  fi
  
 done
 
echo "PIG_JOB_NAME: " $PIG_JOB_NAME
echo "PARAMLIST : " $PARAMLIST

JOB_TYPE=`echo $PIG_JOB_NAME | cut -f2- -d'.'`

if [ "$DEPLOY_MODE" == "AUTO" ]; then
   SCRIPTS=""
   LOGS=""
else
   SCRIPTS="/scripts"
   LOGS="/logs"
fi

if [ "$JOB_PATH" == "-" ]; then
   SCRIPT_DIR=${SCRIPT_PATH}
   LOG_DIR=${LOG_PATH}
else
   SCRIPT_DIR=${SCRIPT_PATH}/${JOB_PATH}${SCRIPTS}
   LOG_DIR=${LOG_PATH}/${JOB_PATH}${LOGS}  
fi    

if [ ! -d "$LOG_DIR" ]; then
   mkdir -p $LOG_DIR
   chmod -R 775 $LOG_DIR
fi 

if [[ "$TYPE" == "pig" ]]; then
     if [ "$JOB_TYPE" == "pig" ]; then
         PIG_SCRIPT=${SCRIPT_DIR}/${PIG_JOB_NAME}
     else
         PIG_SCRIPT=${SCRIPT_DIR}/${PIG_JOB_NAME}.pig
     fi
else
     echo "Exit: not pig job !" 
     exit 1
fi


echo "Pig script name: " $PIG_SCRIPT
echo "Job_Name: " $PIG_JOB_NAME
echo "PARAMLIST : " $PARAMLIST

######## Set the log directory path and the log file name. #########
#LOG_FILE=""$LOG_PATH"/"$PIG_JOB_NAME-$START_TIME-$ID".log"
#echo "Log file : " $LOG_FILE
LOG_FILE=""$LOG_DIR"/"$PIG_JOB_NAME-$START_TIME-$ID".log"
echo "Log file : " $LOG_FILE


EMAIL_REPORT=""$TMP"/"$PIG_JOB_NAME-$START_TIME"-email-report-$ID.txt"
echo "Email report file: " $EMAIL_REPORT

ZEKE_RC=0

job_start_time=$(date +%Y-%m-%d_%T)
#echo $PIG_HEAPSIZE
#$PIG $PARAMLIST $PIG_SCRIPT >> $LOG_FILE 2>&1
#PIG_RC=$?
if [ "$PIG_HEAPSIZE" == ""  ]; then
	 echo $PIG $PARAMLIST $PIG_SCRIPT
	 $PIG $PARAMLIST $PIG_SCRIPT >> $LOG_FILE 2>&1  
	 PIG_RC=$?
else
	 echo PIG_HEAPSIZE=$PIG_HEAPSIZE $PIG $PARAMLIST $PIG_SCRIPT
	 PIG_HEAPSIZE=$PIG_HEAPSIZE $PIG $PARAMLIST $PIG_SCRIPT >> $LOG_FILE 2>&1
	 PIG_RC=$?
fi 
echo PIG RC is $PIG_RC
ZEKE_RC=$PIG_RC
  if [ $PIG_RC -ne 0 ]; then
       echo "TOTAL RETRIES: " $RETRY
       i=0
       while [ $RETRY -gt i ]
       do
         echo "Retry # : " $i
        $PIG $PARAMLIST $PIG_SCRIPT >> $LOG_FILE 2>&1
         RC=$?
         if [ $RC -eq 0 ]; then
           break
         fi
         let i=i+1
       done
 fi


extract_piglog $APP_ID $ZEKE_JOB_NAME $START_TIME $ID $PIG_RC $TMP 


if [[  -e $DETAIL_REPORT ]]; then
   rm $DETAIL_REPORT
fi

if [[  -e $PARAM_FILE ]]; then
   rm $PARAM_FILE
fi

if [[  -e $PARAM_LOG_FILE ]]; then
   rm $PARAM_LOG_FILE
fi

if [[  -e $EMAIL_REPORT ]]; then
 rm $EMAIL_REPORT
fi

if [[  -e $SUMMARY_REPORT ]]; then
 rm $SUMMARY_REPORT
fi

if [[  -e $YARN_LOG_FILE ]]; then
 rm $YARN_LOG_FILE
fi

if [ $ZEKE_RC -ne 0 ]; then
   echo " ZEKE Exit RC : " $ZEKE_RC
         exit 1
fi




####################  wrapper pyspark ################


#!/bin/ksh
#########################################################
# Wrapper Script for PySpark Jobs
# 
# Zeke will invoke this script by passing Zeke Job Name
# as a parameter.
# 
# 
# 
# 
#
# Revision History
# 0.1 02/05/2016 Initial draft.
#
#########################################################
#set -x
ZEKE_JOB_NAME=$1
ROOT_PATH=/u01/datascience/common/bin
. $ROOT_PATH/functions/commonutil.ksh

TMP='/tmp'
ID=$RANDOM

user_id=$(whoami)
server_name=$(hostname)

ENV_FILE="$ROOT_PATH/common.env"
echo "ZEKE_JOB_NAME :" $ZEKE_JOB_NAME
echo "ENV_FILE : " $ENV_FILE 

if [[ -z $ZEKE_JOB_NAME ]]; then
  echo "Error: Missing zeke-job-nam parameter."
  echo "USAGE: wrapper_ds.ksh zeke-job-name "
  exit 1
fi

source $ENV_FILE
if [ $? -ne 0 ]; then
  echo "Error: Missing env file."
  exit 1
fi

sid=$user_id
echo "sid:" $sid
usid=$(echo $sid | tr '[:lower:]' '[:upper:]')
echo "usid : " $usid

/usr/bin/kinit $usid@AETH.AETNA.COM -k -t /users/$sid/$sid.keytab

date=`date -d"-1 days" +%Y-%m-%d`
yr=`echo $date | awk -F\- '{print $1}'`
mth=`echo $date | awk -F\- '{print $2}'`
dy=`echo $date | awk -F\- '{print $3}'`

PROGRESS="InProgress"
COMPLETE="Complete"

START_TIME=$(date  +%Y-%m-%d_%T)
START_TIME_S=$SECONDS

PARAM_LOG_FILE=""$TMP"/"$ZEKE_JOB_NAME-$START_TIME-$ID-PARAMS".log"
echo "Log file : " $PARAM_LOG_FILE

PARAM_FILE=""$TMP"/"$ZEKE_JOB_NAME-$START_TIME-$ID-PARAM".txt"
echo "Param file : " $PARAM_FILE

ERRORLIST_FILE=""$TMP"/"$ZEKE_JOB_NAME-$START_TIME-$ID-ERRORLIST".txt"
typeset -A error_msgs

##########  Get the job attributes from the Job Master table ############

SQLSTMT="select j.job_name, a.email , a.script_path , a.log_path , j.tools_used , j.retry_attempt ,COALESCE(nullif(j.job_path,''),'-') ,coalesce(nullif(j.op_stats_table,''),'NULL') , a.app_name , j.job_id , a.yarn_logs ,a.app_id, j.batchid, j.batchname  from $APP_TABLE as a join $JOB_TABLE as j on j.app_id=a.app_id  where j.Zeke_Job_Name='$ZEKE_JOB_NAME';"

echo "SQL statement: " $SQLSTMT
execute_sql "$SQLSTMT" SQLRESULT RC
  echo "RC: " $RC
  if [ $RC -ne 0 ]; then
    echo "inside" 
    EMAIL_REPORT=""$TMP"/temp.txt"
    EMAIL_TO=$EMAIL_ADDR

    echo "Job $ZEKE_JOB_NAME failed to load parameters."
    echo -e "Message: Failed to load parameters from app & job tables." >> $EMAIL_REPORT
    report_job_run "Failed "

    echo "EMAIL_REPORT :" $EMAIL_REPORT
    echo "EMIL_TO : " $EMAIL_TO

    if [[  -e $EMAIL_REPORT ]]; then
     rm $EMAIL_REPORT
    fi

    exit 1 
  fi
echo $SQLRESULT | read PYSPARK_JOB_NAME EMAIL_TO SCRIPT_PATH LOG_PATH TYPE RETRY JOB_PATH STATS_TABLE APP_NAME JOB_ID YARN_LOGS_LOC APP_ID BATCH_ID BATCH_NAME
echo JOB_NAME is $PYSPARK_JOB_NAME
echo EMAIL_TO is $EMAIL_TO
echo SCRIPT_PATH is $SCRIPT_PATH
echo LOG_PATH is $LOG_PATH
echo JOB_PATH is $JOB_PATH
echo TYPE is $TYPE
echo RETRY is  $RETRY
echo STATS_TABLE is $STATS_TABLE
echo APP_NAME is $APP_NAME
echo JOB_ID is $JOB_ID
echo YARN_LOGS_LOC is $YARN_LOGS_LOC
echo APP_ID is $APP_ID
echo BATCH_ID is $BATCH_ID
echo BATCH_NAME is $BATCH_NAME


##########  Get the cycle  attributes from the Job Cycle table ############
SQLSTMT="select status,cycle_id,app_id,batchid from $CYCLE_TABLE where app_id='$APP_ID'and batchid='$BATCH_ID' and status='InProgress' order by cycle_id desc limit 1 ;"

echo SQLSTMT is $SQLSTMT
execute_sql "$SQLSTMT" SQLRESULT RC
  echo "RC: " $RC
  if [ $RC -ne 0 ]; then
    echo "Eror retrieving status: "
  fi 

echo $SQLRESULT | read STATUS CYCLE_ID APP_ID B_ID
echo STATUS is $STATUS
echo CYCLE_ID is $CYCLE_ID
echo APP_ID is $APP_ID
echo CYCLE BATCH_ID is $B_ID

if [[ "$STATUS" != $PROGRESS ]]; then
        echo Error: Cycle is not in progress, Please open the cycle and rerun !
        exit 1
fi

#########  Get  error list from the error  Master table ############
SQLSTMT="select script_type, error from $ERROR_TABLE where script_type='pyspark'";
echo "SQL statement: " $SQLSTMT
SQLRESULT=`mysql --skip-column-names -h$HOST --port $PORT  -u$USER -p$PW -e "$SQLSTMT"`
RC=$?
echo "RC: " $RC
if [ $RC -ne 0 ]; then
    echo "inside"
    EMAIL_REPORT=""$TMP"/temp.txt"
    EMAIL_TO=$EMAIL_ADDR

    echo "Job $ZEKE_JOB_NAME failed to load error list."
    echo -e "Message: Failed to load parameters from error table." >> $EMAIL_REPORT
    report_job_run "Failed "

    echo "EMAIL_REPORT :" $EMAIL_REPORT
    echo "EMIL_TO : " $EMAIL_TO

    if [[  -e $EMAIL_REPORT ]]; then
     rm $EMAIL_REPORT
    fi

    exit 1
fi
echo "$SQLRESULT" >> $ERRORLIST_FILE
k=0
######### Construct error list #########
cat $ERRORLIST_FILE | while read line
 do
  k=`expr $k + 1`
  script_type=`echo $line | cut -f1 -d' '`
  error=`echo $line | cut -f2- -d' '`
  if [[ "$script_type" == 'pyspark' ]]; then
        echo script type is pyspark....
        error_msgs[$k]=$error
  fi
done


#########  Get the param  attributes from the Job Param table ############
SQLSTMT="select p.param_name,p.param_value from $PARAM_TABLE as p join $JOB_TABLE as j on p.job_id=j.job_id where j.Zeke_Job_Name='$ZEKE_JOB_NAME';"
echo "SQL statement: " $SQLSTMT
SQLRESULT=`mysql --skip-column-names -h$HOST --port $PORT  -u$USER -p$PW -e "$SQLSTMT"`
RC=$?
echo "RC: " $RC
if [ $RC -ne 0 ]; then
    echo "inside" 
    EMAIL_REPORT=""$TMP"/temp.txt"
    EMAIL_TO=$EMAIL_ADDR

    echo "Job $ZEKE_JOB_NAME failed to load parameters."
    echo -e "Message: Failed to load parameters from param table." >> $EMAIL_REPORT
    report_job_run "Failed "

    echo "EMAIL_REPORT :" $EMAIL_REPORT
    echo "EMIL_TO : " $EMAIL_TO

    if [[  -e $EMAIL_REPORT ]]; then
     rm $EMAIL_REPORT
    fi

    exit 1 
  fi

echo "$SQLRESULT" >> $PARAM_FILE

######### Construct parameter list #########
typeset -A params_hash


cat $PARAM_FILE | while read line
 do
  echo "line:" $line
  F1=`echo $line | cut -f1 -d' '`
  F2=`echo $line | cut -f2- -d' '`
   if [[ ! -z $F1 && ! -z $F2 ]]; then
     X1=`echo $F2 | cut -c1-1`
      if [[ "$X1" == '`' ]]; then
         eval F2VAL=$F2
         F2=$F2VAL
          echo "F2VAL : " $F2
       fi
      fi
 
#  if [[ "$F1" == 'yaml_file' ]]; then
#	PARAMLIST=$SCRIPT_PATH/$F2
#	continue
#  fi
  if [[ "$F1" == 'pyspark' ]]; then
	PYSPARK=$F2
	continue
  fi
  
    if [[ "$F1" == 'remote' ]]; then
	REMOTE=$F2
	continue
  fi
  
  if [[ "$F1" == 'remote_path' ]]; then
        REMOTE_PATH=$F2
        continue
  fi

  C2=`echo $F1 | head  -c2`
  
  if [[ "$C2" == '--' ]]; then
       STANDARD_PARAMS=$(echo $STANDARD_PARAMS $F1 $F2)
       continue
  fi
  echo C2 is $C2  

  params_hash[$F1]=$F2
 
 done
echo STANDART_PARAMS: $STANDARD_PARAMS
let a=0
let b=1
while [[ $a -lt ${#params_hash[*]} ]] ; do
	arg=${params_hash["arg-$b"]}
	echo arg is : $arg
        if [[ ! -z $arg ]] then;
       		PARAMLIST=$(echo $PARAMLIST $arg)
        fi

	let a+=1
	let b+=1
done
echo PARAMLIST is $PARAMLIST

JOB_TYPE=`echo $PYSPARK_JOB_NAME | cut -f2- -d'.'`

if [ "$DEPLOY_MODE" == "AUTO" ]; then
   SCRIPTS=""
   LOGS=""
else
   SCRIPTS="/scripts"
   LOGS="/logs"
fi

if [ "$JOB_PATH" == "-" ]; then
   SCRIPT_DIR=${SCRIPT_PATH}
   LOG_DIR=${LOG_PATH}
else
   SCRIPT_DIR=${SCRIPT_PATH}/${JOB_PATH}${SCRIPTS}
   LOG_DIR=${LOG_PATH}/${JOB_PATH}${LOGS}  
fi 

if [ ! -d "$LOG_DIR" ]; then
   mkdir -p $LOG_DIR
   chmod -R 775 $LOG_DIR
fi

if [[ "$TYPE" == "pyspark" ]]; then
     if [ "$JOB_TYPE" == "pyspark" ]; then
         FILE_NAME_=${SCRIPT_DIR}/${PYSPARK_JOB_NAME}
     else
         FILE_NAME_=${SCRIPT_DIR}/${PYSPARK_JOB_NAME}.py
     fi
else
     echo "Exit: not pyspark job !" 
     exit 1
fi   


#FILE_NAME_=$SCRIPT_PATH/"$PYSPARK_JOB_NAME".py

echo Job_Name:  $PYSPARK_JOB_NAME
echo Param List : $PARAMLIST
echo FILE_NAME_ $FILE_NAME_
echo REMOTE is $REMOTE
echo REMOTE PATH is $REMOTE_PATH

####### Set the log directory path and the log file name. ##########
#LOG_FILE=""$LOG_PATH"/"$PYSPARK_JOB_NAME-$START_TIME-$ID".log"

LOG_FILE=""$LOG_DIR"/"$PYSPARK_JOB_NAME-$START_TIME-$ID".log"
echo "Log file : " $LOG_FILE

EMAIL_REPORT=""$TMP"/"$PYSPARK_JOB_NAME-$START_TIME"-email-report-$ID.txt"
echo "Email report file: " $EMAIL_REPORT

DETAIL_REPORT=""$TMP"/"$PYSPARK_JOB_NAME"-detail-report-"$ID".txt"

job_start_time=$(date +%Y-%m-%d_%T)

 if [[ "$REMOTE" == 'Y' ]]; then
        echo Executing job on remote server...
        REMOTE_HOST=$(echo $REMOTE_PATH|awk '$0=$2' FS=@ RS=:)
	REMOTE_PATH_=`echo $REMOTE_PATH | cut -f 2 -d':'`
	REMOTE_HOST1=`echo $REMOTE_PATH | cut -f1 -d':'`
	echo REMOTE PATH: $REMOTE_PATH_
        echo RECMOTE HOST1: $REMOTE_HOST1
        REMOTE_FILE_NAME_=$REMOTE_PATH_/"$PYSPARK_JOB_NAME".py
        SCP_FILES=$SCRIPT_PATH/"$PYSPARK_JOB_NAME"*
        REMOTE_YAML_FILE_NAME_=$REMOTE_PATH_/"$PYSPARK_JOB_NAME".yaml
        echo remote FILE_NAME_ $REMOTE_FILE_NAME_
	echo SCP_FILES $SCP_FILES	
        scp $SCP_FILES $REMOTE_PATH/
        #echo ssh : $REMOTE_HOST1 "/usr/bin/kinit $remoteuid@AETH.AETNA.COM -k -t /users/$remotesid/$remotesid.keytab && $PYSPARK $STANDARD_PARAMS $REMOTE_FILE_NAME_ $REMOTE_YAML_FILE_NAME_ $PARAMLIST"
        #ssh $REMOTE_HOST1 "source $REMOTE_PATH_/env.sh && $PYSPARK $STANDARD_PARAMS $REMOTE_FILE_NAME_ $REMOTE_YAML_FILE_NAME_ $PARAMLIST" >> $LOG_FILE 2>&1
				remotesid=`echo $(echo $REMOTE_HOST1|cut -f1 -d'@')`
				echo $remotesid
				remoteuid=$(echo $remotesid | tr '[:lower:]' '[:upper:]')
				echo $remoteuid
				echo ssh : $REMOTE_HOST1 "/usr/bin/kinit $remoteuid@AETH.AETNA.COM -k -t /users/$remotesid/$remotesid.keytab && $PYSPARK $STANDARD_PARAMS $REMOTE_FILE_NAME_ $REMOTE_YAML_FILE_NAME_ $PARAMLIST"
        #ssh $REMOTE_HOST1 "source ~/.bashrc &&  && $PYSPARK $STANDARD_PARAMS $REMOTE_FILE_NAME_ $REMOTE_YAML_FILE_NAME_ $PARAMLIST" >> $LOG_FILE 2>&1
        ssh $REMOTE_HOST1 "/usr/bin/kinit $remoteuid@AETH.AETNA.COM -k -t /users/$remotesid/$remotesid.keytab && $PYSPARK $STANDARD_PARAMS $REMOTE_FILE_NAME_ $REMOTE_YAML_FILE_NAME_ $PARAMLIST" >> $LOG_FILE 2>&1
        RC=$?
        echo PySpark remote RC: $RC
      else
        $PYSPARK $STANDARD_PARAMS $FILE_NAME_ $PARAMLIST >> $LOG_FILE 2>&1
	RC=$?
        echo PySpark RC: $RC
  fi

j=0
while [ $RETRY -gt j ]
 do
	for index in "${!error_msgs[@]}"; do
		error=${error_msgs["$index"]}

		if [ $(cat $LOG_FILE | grep -i "$error" | wc -w) -gt 0 ]; then
			 echo Log file contains error $error
			 echo "Retry # : " $j
			 LOG_FILE=$LOG_FILE-RESTART-$j

 			 if [[ "$REMOTE" == 'Y' ]]; then
        			ssh $REMOTE_HOST $PYSPARK $STANDARD_PARAMS $FILE_NAME_ $PARAMLIST >> $LOG_FILE 2>&1
        			RC=$?
			 else
			 	$PYSPARK $FILE_NAME_ $PARAMLIST >> $LOG_FILE 2>&1
        			RC=$?
			 fi 
			 break
		else
			echo Log file does not contain error : $error
		fi
	done

	let j=j+1
done
echo RC is  $RC
ZEKE_RC=$RC

END_TIME_S=$SECONDS
time_taken=$((END_TIME_S- START_TIME_S))
message=""

#RC=2
#LOG_FILE="/u01/datascience/Sample/logs/sample_python.log"
echo LOG_FILE is $LOG_FILE

if [ $RC -ne 0 ]; then
	echo "Job Failed.."
	JOB_STATUS="Failed"
                
	if [ $(cat $LOG_FILE | grep -i "Exception" | wc -w) -gt 0 ]; then
		message=$(cat $LOG_FILE | grep -i "Exception")
		message=$(echo $message | sed -s "s/'//g")
	fi
	if [ $(cat $LOG_FILE | grep -i "Error" | wc -w) -gt 0 ]; then
		message=$(cat $LOG_FILE | grep -i "Error")
		message=$(echo $message | sed -s "s/'//g")
	fi
        echo "message: " $message
	if [ $(cat $LOG_FILE | grep -i "Submitted application" | wc -w) -gt 0 ]; then
	   application=$(cat $LOG_FILE | grep -i "Submitted application")
	   echo "application : " $application
	   app_id=$(echo $application | cut -d" " -f7)
	   YARN_FILE_NAME="$ZEKE_JOB_NAME-$ID-$app_id.log"
	   YARN_LOG_FILE=""$LOG_PATH"/"$YARN_FILE_NAME""
	   echo "app_id :  " $app_id
	   echo "sleeping for 180 sec"
	   sleep 180 
	   yarn logs -applicationId $app_id >> $YARN_LOG_FILE
	   RC=$?
	   echo "Yarn RC : " $RC
	   if [ $RC -ne 0 ]; then
		 echo "Yarn retry:"
		 sleep 180
		 yarn logs -applicationId $app_id >> $YARN_LOG_FILE
	   fi
	   #hadoop fs -put $YARN_LOG_FILE $YARN_LOGS_LOC
	fi
	if [[ -z "$app_id" ]]; then
			  echo -e "App Name: $APP_NAME\nJob Name: $PYSPARK_JOB_NAME\nBatch Name: $BATCH_NAME\nLog File: $LOG_FILE\nStatus: $JOB_STATUS\nMessage: $message\nTime Taken: $time_taken(sec)\nDate: $date\n" >> $EMAIL_REPORT
	else
		   echo -e "App Name: $APP_NAME\nJob Name: $PYSPARK_JOB_NAME\nBatch Name: $BATCH_NAME\nLog File: $LOG_FILE\nStatus: $JOB_STATUS\nMessage: $message\nTime Taken: $time_taken(sec)\nDate: $date\nYarn Log File: $YARN_FILE_NAME\nHDFS Loc: $YARN_LOGS_LOC\nAPP ID:$app_id" >> $EMAIL_REPORT
	fi
		report_job_run "Failed "
else
	JOB_STATUS="Success"
	echo  "Job completed successfully.."
	echo -e "App Name: $APP_NAME\nJob Name: $PYSPARK_JOB_NAME\nBatchName: $BATCH_NAME\nLog File: $LOG_FILE\nStatus: $JOB_STATUS\nTime Taken: $time_taken(sec)\nDate: $date\n"  >> $EMAIL_REPORT
		report_job_run "Success"
fi

END_TIME=$(date +"%Y-%m-%d %T")

##########   Load stats into Detail table ############
#SQLSTMT="insert into $DETAIL_TABLE(job_id,Total_Size,Time_Take,cycle_id) values ($JOB_ID, NULL, '$time_taken','$CYCLE_ID');"

#echo "SQLSTMT : " $SQLSTMT
#execute_sql "$SQLSTMT" SQLRESULT RC
#RC=$?
#echo "DETAIL TABLE RC :" $RC
message=${message:0:5000}
##########   Load stats into Summary table ############
SQLSTMT="insert into $SUMMARY_TABLE (job_id,job_name,target,user_id,server_name,job_start_time,job_end_time,message,status,cycle_id) values ($JOB_ID,'$PYSPARK_JOB_NAME','$TYPE','$user_id','$server_name','$START_TIME','$END_TIME','$message', '$JOB_STATUS','$CYCLE_ID');"

echo "SQLSTMT : " $SQLSTMT
execute_sql "$SQLSTMT" SQLRESULT RC
RC=$?
echo "SUMMARY TABLE RC :" $RC


if [[  -e $ERRORLIST_FILE ]]; then
 rm $ERRORLIST_FILE
fi

if [[  -e $DETAIL_REPORT ]]; then
   rm $DETAIL_REPORT
fi

if [[  -e $PARAM_FILE ]]; then
   rm $PARAM_FILE
fi

if [[  -e $PARAM_LOG_FILE ]]; then
   rm $PARAM_LOG_FILE
fi

if [[  -e $YARN_LOG_FILE ]]; then
   rm $YARN_LOG_FILE 
fi

if [[  -e $EMAIL_REPORT ]]; then
 rm $EMAIL_REPORT
fi

if [ $ZEKE_RC -ne 0 ]; then
   echo " ZEKE Exit RC : " $ZEKE_RC
         exit 1
fi

                
                

	
###############   wrapper python ######

#!/bin/ksh
#########################################################
# Wrapper Script for Python Jobs
# 
# Zeke will invoke this script by passing Zeke Job Name
# as a parameter.
# 
# 
# 
# 
#
# Revision History
# 0.1 01/25/2016 Initial draft.
#
#########################################################
#set -x
ZEKE_JOB_NAME=$1
ROOT_PATH='/u01/datascience/common/bin'
. $ROOT_PATH/functions/commonutil.ksh

TMP='/tmp'
ID=$RANDOM

user_id=$(whoami)
server_name=$(hostname)

ENV_FILE="$ROOT_PATH/common.env"
echo "ZEKE_JOB_NAME :" $ZEKE_JOB_NAME
echo "ENV_FILE : " $ENV_FILE 

if [[ -z $ZEKE_JOB_NAME ]]; then
  echo "Error: Missing zeke-job-nam parameter."
  echo "USAGE: wrapper_ds.ksh zeke-job-name "
  exit 1
fi

source $ENV_FILE
if [ $? -ne 0 ]; then
  echo "Error: Missing env file."
  exit 1
fi

sid=$user_id
echo "sid:" $sid
usid=$(echo $sid | tr '[:lower:]' '[:upper:]')
echo "usid : " $usid

/usr/bin/kinit $usid@AETH.AETNA.COM -k -t /users/$sid/$sid.keytab

date=`date -d"-1 days" +%Y-%m-%d`
yr=`echo $date | awk -F\- '{print $1}'`
mth=`echo $date | awk -F\- '{print $2}'`
dy=`echo $date | awk -F\- '{print $3}'`

PROGRESS="InProgress"
COMPLETE="Complete"

START_TIME=$(date  +%Y-%m-%d_%T)
START_TIME_S=$SECONDS

PARAM_LOG_FILE=""$TMP"/"$ZEKE_JOB_NAME-$START_TIME-$ID-PARAMS".log"
echo "Log file : " $PARAM_LOG_FILE

PARAM_FILE=""$TMP"/"$ZEKE_JOB_NAME-$START_TIME-$ID-PARAM".txt"
echo "Param file : " $PARAM_FILE

ERRORLIST_FILE=""$TMP"/"$ZEKE_JOB_NAME-$START_TIME-$ID-ERRORLIST".txt"
typeset -A error_msgs

##########  Get the job attributes from the Job Master table ############

SQLSTMT="select j.job_name, a.email , a.script_path , a.log_path , COALESCE(nullif(j.job_path,''),'-'), j.tools_used , j.retry_attempt ,coalesce(nullif(j.op_stats_table,''),'NULL') , a.app_name , j.job_id , a.yarn_logs ,a.app_id, j.batchid, j.batchname  from $APP_TABLE as a join $JOB_TABLE as j on j.app_id=a.app_id  where j.Zeke_Job_Name='$ZEKE_JOB_NAME';"

echo "SQL statement: " $SQLSTMT
execute_sql "$SQLSTMT" SQLRESULT RC
  echo "RC: " $RC
  if [ $RC -ne 0 ]; then
    echo "inside" 
    EMAIL_REPORT=""$TMP"/temp.txt"
    EMAIL_TO=$EMAIL_ADDR

    echo "Job $ZEKE_JOB_NAME failed to load parameters."
    echo -e "Message: Failed to load parameters from app & job tables." >> $EMAIL_REPORT
    report_job_run "Failed "

    echo "EMAIL_REPORT :" $EMAIL_REPORT
    echo "EMIL_TO : " $EMAIL_TO

    if [[  -e $EMAIL_REPORT ]]; then
     rm $EMAIL_REPORT
    fi

    exit 1 
  fi
echo $SQLRESULT | read PYTHON_JOB_NAME EMAIL_TO SCRIPT_PATH LOG_PATH JOB_PATH TYPE RETRY STATS_TABLE APP_NAME JOB_ID YARN_LOGS_LOC APP_ID BATCH_ID BATCH_NAME
echo JOB_NAME is $PYTHON_JOB_NAME
echo EMAIL_TO is $EMAIL_TO
echo SCRIPT_PATH is $SCRIPT_PATH
echo LOG_PATH is $LOG_PATH
echo JOB_PATH is $JOB_PATH
echo TYPE is $TYPE
echo RETRY is  $RETRY
echo STATS_TABLE is $STATS_TABLE
echo APP_NAME is $APP_NAME
echo JOB_ID is $JOB_ID
echo YARN_LOGS_LOC is $YARN_LOGS_LOC
echo APP_ID is $APP_ID
echo BATCH_ID is $BATCH_ID
echo BATCH_NAME is $BATCH_NAME


##########  Get the cycle  attributes from the Job Cycle table ############
SQLSTMT="select status,cycle_id,app_id,batchid from $CYCLE_TABLE where app_id='$APP_ID'and batchid='$BATCH_ID' and status='InProgress' order by cycle_id desc limit 1 ;"

echo SQLSTMT is $SQLSTMT
execute_sql "$SQLSTMT" SQLRESULT RC
  echo "RC: " $RC
  if [ $RC -ne 0 ]; then
    echo "Eror retrieving status: "
  fi 

echo $SQLRESULT | read STATUS CYCLE_ID APP_ID B_ID
echo STATUS is $STATUS
echo CYCLE_ID is $CYCLE_ID
echo APP_ID is $APP_ID
echo CYCLE BATCH_ID is $B_ID

if [[ "$STATUS" != $PROGRESS ]]; then
	echo Error: Cycle is not in progress, Please open the cycle and rerun !
        exit 1
fi

#########  Get  error list from the error  Master table ############
SQLSTMT="select script_type, error from $ERROR_TABLE where script_type='python'";
echo "SQL statement: " $SQLSTMT
SQLRESULT=`mysql --skip-column-names -h$HOST --port $PORT  -u$USER -p$PW -e "$SQLSTMT"`
RC=$?
echo "RC: " $RC
if [ $RC -ne 0 ]; then
    echo "inside"
    EMAIL_REPORT=""$TMP"/temp.txt"
    EMAIL_TO=$EMAIL_ADDR

    echo "Job $ZEKE_JOB_NAME failed to load error list."
    echo -e "Message: Failed to load parameters from error table." >> $EMAIL_REPORT
    report_job_run "Failed "

    echo "EMAIL_REPORT :" $EMAIL_REPORT
    echo "EMIL_TO : " $EMAIL_TO

    if [[  -e $EMAIL_REPORT ]]; then
     rm $EMAIL_REPORT
    fi

    exit 1
fi
echo "$SQLRESULT" >> $ERRORLIST_FILE
k=0
######### Construct error list #########
cat $ERRORLIST_FILE | while read line
 do
  k=`expr $k + 1`
  script_type=`echo $line | cut -f1 -d' '`
  error=`echo $line | cut -f2- -d' '`
  if [[ "$script_type" == 'python' ]]; then
        echo script type is python....
        error_msgs[$k]=$error
  fi
done


#########  Get the param  attributes from the Job Param table ############
SQLSTMT="select p.param_name,p.param_value from $PARAM_TABLE as p join $JOB_TABLE as j on p.job_id=j.job_id where j.Zeke_Job_Name='$ZEKE_JOB_NAME';"
echo "SQL statement: " $SQLSTMT
SQLRESULT=`mysql --skip-column-names -h$HOST --port $PORT  -u$USER -p$PW -e "$SQLSTMT"`
RC=$?
echo "RC: " $RC
if [ $RC -ne 0 ]; then
    echo "inside" 
    EMAIL_REPORT=""$TMP"/temp.txt"
    EMAIL_TO=$EMAIL_ADDR

    echo "Job $ZEKE_JOB_NAME failed to load parameters."
    echo -e "Message: Failed to load parameters from param table." >> $EMAIL_REPORT
    report_job_run "Failed "

    echo "EMAIL_REPORT :" $EMAIL_REPORT
    echo "EMIL_TO : " $EMAIL_TO

    if [[  -e $EMAIL_REPORT ]]; then
     rm $EMAIL_REPORT
    fi

    exit 1 
  fi

echo "$SQLRESULT" >> $PARAM_FILE
cat $PARAM_FILE | while read line
 do
  F1=`echo $line | cut -f1 -d' '`
  F2=`echo $line | cut -f2- -d' '`
 
  if [[ "$F1" == 'type' ]]; then
	CLASS=$F2	
	break
  fi

 done 

echo  TYPE is $CLASS

######### Construct parameter list #########

if [[ "$CLASS" == 'streaming' ]]; then
	cat $PARAM_FILE | while read line
	 do
	  F1=`echo $line | cut -f1 -d' '`
	  F2=`echo $line | cut -f2- -d' '`
	  
          if [[ "$F1" == 'type' ]]; then
		continue
          fi

	  if [[ "$F1" == '-output' ]]; then
		ODIR=$F2	
	  fi
	  
	  if [[ ! -z $F1 && ! -z $F2 ]]; then
	     C1=`echo $F2 | cut -c1-1`
	      if [[ "$C1" == '`' ]]; then
		 eval F2VAL=$F2
		  echo "F2VAL : " $F2VAL
		 PARAMLIST=$(echo $PARAMLIST $F1 $F2VAL)
	      else
		 PARAMLIST=$(echo $PARAMLIST $F1 $F2)
	    fi
	  fi
	 done
fi 
typeset -A params_hash

JOB_TYPE=`echo $PYTHON_JOB_NAME | cut -f2- -d'.'`

if [ "$DEPLOY_MODE" == "AUTO" ]; then
   SCRIPTS=""
   LOGS=""
else
   SCRIPTS="/scripts"
   LOGS="/logs"
fi

if [ "$JOB_PATH" == "-" ]; then
   SCRIPT_DIR=${SCRIPT_PATH}
   LOG_DIR=${LOG_PATH}
else
   SCRIPT_DIR=${SCRIPT_PATH}/${JOB_PATH}${SCRIPTS}
   LOG_DIR=${LOG_PATH}/${JOB_PATH}${LOGS}  
fi 

if [ ! -d "$LOG_DIR" ]; then
   mkdir -p $LOG_DIR
   chmod -R 775 $LOG_DIR
fi 

if [[ "$CLASS" == "python" ]]; then
     if [ "$JOB_TYPE" == "py" ]; then
         PYTHON_FILE=${SCRIPT_DIR}/${PYTHON_JOB_NAME}
     else
         PYTHON_FILE=${SCRIPT_DIR}/${PYTHON_JOB_NAME}.py
     fi

	 PARAMLIST=$PYTHON_FILE

	cat $PARAM_FILE | while read line
	 do
	  F1=`echo $line | cut -f1 -d' '`
	  F2=`echo $line | cut -f2- -d' '`
	  
          if [[ "$F1" == 'type' ]]; then
		continue
          fi
	      if [[ "$F1" == 'remote_path' ]]; then
              REMOTE_PATH=$F2
	       continue
          fi
          if [[ "$F1" == 'remote' ]]; then
              REMOTE=$F2
	       continue
          fi
	      if [[ "$F1" == 'files' ]]; then
	         IFS=","
	         set -A files $F2
	         echo tab1 ${files[0]}
	         len=${#files[@]}
	      fi
          unset IFS
          params_hash[$F1]=$F2	  
        done
	let a=0
	let b=1
	while [[ $a -lt ${#params_hash[*]} ]] ; do
		arg=${params_hash["arg-$b"]}
		echo arg is : $arg
		PARAMLIST=$(echo $PARAMLIST $arg)
		let a+=1
		let b+=1
	done
fi
echo PARAMLIST is $PARAMLIST
echo REMOTE is $REMOTE
echo REMOTE PATH is $REMOTE_PATH
####### Set the log directory path and the log file name. ##########

#LOG_FILE=""$LOG_PATH"/"$PYTHON_JOB_NAME-$START_TIME-$ID".log"
LOG_FILE=""$LOG_DIR"/"$PYTHON_JOB_NAME-$START_TIME-$ID".log"
echo "Log file : " $LOG_FILE

EMAIL_REPORT=""$TMP"/"$PYTHON_JOB_NAME-$START_TIME"-email-report-$ID.txt"
echo "Email report file: " $EMAIL_REPORT

DETAIL_REPORT=""$TMP"/"$PYTHON_JOB_NAME"-detail-report-"$ID".txt"

job_start_time=$(date +%Y-%m-%d_%T)

if [[ "$CLASS" == 'python' ]]; then
  if [[ "$REMOTE" == 'Y' ]]; then
	echo Executing job on remote server...
	REMOTE_HOST=$(echo $REMOTE_PATH| cut -d ':' -f1)
	echo $PYTHON_FILE $REMOTE_PATH
	scp $PYTHON_FILE $REMOTE_PATH/
        if [[ ! -z "${files}" ]]; then
            c=0
	    while [[ $c -le $len-1 ]]; do
		FILE2="$SCRIPT_PATH/${files[$c]}"
		scp $FILE2 $REMOTE_PATH/
       		(( c++ ))
	    done
        fi
	PYTHON_FILE=$(echo $REMOTE_PATH | cut -d":" -f2)
	PYTHON_FILE=$PYTHON_FILE$PYTHON_JOB_NAME.py
	echo ssh : $REMOTE_HOST $PYTHON $PYTHON_FILE $PARAMLIST
	ssh $REMOTE_HOST $PYTHON $PYTHON_FILE $PARAMLIST >> $LOG_FILE 2>&1
 	RC=$?
 	echo Python remote RC: $RC
      else	
        $PYTHON $PARAMLIST >> $LOG_FILE 2>&1 
 	RC=$?
 	echo Python RC: $RC
  fi
fi

if [[ "$CLASS" == 'streaming' ]]; then
	echo ODIR : $ODIR
	echo CLASS : $CLASS
	hadoop fs -rm -r $ODIR
	RC=$?
	echo RC1: $RC
	hadoop $PARAMLIST >> $LOG_FILE 2>&1
	RC=$?
	echo RC2: $RC
fi

j=0
while [ $RETRY -gt j ]
 do
	for index in "${!error_msgs[@]}"; do
		error=${error_msgs["$index"]}

		if [ $(cat $LOG_FILE | grep -i "$error" | wc -w) -gt 0 ]; then
			 echo Log file contains error $error
			 echo "Retry # : " $j
			 LOG_FILE=$LOG_FILE-RESTART-$j

			 if [[ "$CLASS" == 'streaming' ]]; then
			 	hadoop $PARAMLIST >> $LOG_FILE 2>&1
				 RC=$?
			 fi
			 if [[ "$CLASS" == 'python' ]]; then
  				if [[ "$REMOTE" == 'Y' ]]; then
					ssh $REMOTE_HOST $PYTHON $PYTHON_FILE $PARAMLIST >> $LOG_FILE 2>&1
					RC=$?
					echo Python Remote RC: $RC
				else
					$PYTHON $PARAMLIST >> $LOG_FILE 2>&1 
					RC=$?
					echo Python RC: $RC
				fi
			 fi
			 break
		else
			echo Log file does not contain error : $error
		fi
	done

	let j=j+1
done
echo RC is  $RC
ZEKE_RC=$RC

END_TIME_S=$SECONDS
time_taken=$((END_TIME_S- START_TIME_S))
message=""

#RC=2
#LOG_FILE="/u01/datascience/Sample/logs/sample_python.log"
echo LOG_FILE is $LOG_FILE

if [ $RC -ne 0 ]; then
	echo "Job Failed.."
	JOB_STATUS="Failed"
                
	if [ $(cat $LOG_FILE | grep -i "FAILED" | wc -w) -gt 0 ]; then
		message=$(cat $LOG_FILE | grep -i "FAILED")
		message=$(echo $message | sed -s "s/'//g")
	fi

	if [ $(cat $LOG_FILE | grep -i "ERROR" | wc -w) -gt 0 ]; then
		message=$(cat $LOG_FILE | grep -i "ERROR")
		message=$(echo $message | sed -s "s/'//g")
		echo "message: " $message
	fi

	if [ $(cat $LOG_FILE | grep -i "Submitted application" | wc -w) -gt 0 ]; then
	   application=$(cat $LOG_FILE | grep -i "Submitted application")
	   echo "application : " $application
	   app_id=$(echo $application | cut -d" " -f7)
	   YARN_FILE_NAME="$ZEKE_JOB_NAME-$ID-$app_id.log"
	   YARN_LOG_FILE=""$LOG_PATH"/"$YARN_FILE_NAME""
	   echo "app_id :  " $app_id
	   echo "sleeping for 180 sec"
	   sleep 180 
	   yarn logs -applicationId $app_id >> $YARN_LOG_FILE
	   RC=$?
	   echo "Yarn RC : " $RC
	   if [ $RC -ne 0 ]; then
		 echo "Yarn retry:"
		 sleep 180
		 yarn logs -applicationId $app_id >> $YARN_LOG_FILE
	   fi
	   hadoop fs -put $YARN_LOG_FILE $YARN_LOGS_LOC
	fi
	if [[ -z "$app_id" ]]; then
			  echo -e "App Name: $APP_NAME\nJob Name: $PYTHON_JOB_NAME\nBatch Name: $BATCH_NAME\nLog File: $LOG_FILE\nStatus: $JOB_STATUS\nMessage: $message\nTime Taken: $time_taken(sec)\nDate: $date\n" >> $EMAIL_REPORT
	else
		   echo -e "App Name: $APP_NAME\nJob Name: $PYTHON_JOB_NAME\nBatch Name: $BATCH_NAME\nLog File: $LOG_FILE\nStatus: $JOB_STATUS\nMessage: $message\nTime Taken: $time_taken(sec)\nDate: $date\nYarn Log File: $YARN_FILE_NAME\nHDFS Loc: $YARN_LOGS_LOC\nAPP ID:$app_id" >> $EMAIL_REPORT
	fi
		report_job_run "Failed "
else
	JOB_STATUS="Success"
	echo  "Job completed successfully.."
	echo -e "App Name: $APP_NAME\nJob Name: $PYTHON_JOB_NAME\nBatchName: $BATCH_NAME\nLog File: $LOG_FILE\nStatus: $JOB_STATUS\nTime Taken: $time_taken(sec)\nDate: $date\n"  >> $EMAIL_REPORT
		report_job_run "Success"
fi

END_TIME=$(date +"%Y-%m-%d %T")

##########   Load stats into Detail table ############
#SQLSTMT="insert into $DETAIL_TABLE(job_id,Total_Size,Time_Take,cycle_id) values ($JOB_ID, NULL, '$time_taken','$CYCLE_ID');"
#
#echo "SQLSTMT : " $SQLSTMT
#execute_sql "$SQLSTMT" SQLRESULT RC
#RC=$?
#echo "DETAIL TABLE RC :" $RC
message=${message:0:5000}
##########   Load stats into Summary table ############
SQLSTMT="insert into $SUMMARY_TABLE (job_id,job_name,target,user_id,server_name,job_start_time,job_end_time,message,status,cycle_id) values ($JOB_ID,'$PYTHON_JOB_NAME','$TYPE','$user_id','$server_name','$START_TIME','$END_TIME','$message', '$JOB_STATUS','$CYCLE_ID');"

echo "SQLSTMT : " $SQLSTMT
execute_sql "$SQLSTMT" SQLRESULT RC
RC=$?
echo "SUMMARY TABLE RC :" $RC


if [[  -e $ERRORLIST_FILE ]]; then
 rm $ERRORLIST_FILE
fi

if [[  -e $DETAIL_REPORT ]]; then
   rm $DETAIL_REPORT
fi

if [[  -e $PARAM_FILE ]]; then
   rm $PARAM_FILE
fi

if [[  -e $PARAM_LOG_FILE ]]; then
   rm $PARAM_LOG_FILE
fi

if [[  -e $YARN_LOG_FILE ]]; then
   rm $YARN_LOG_FILE 
fi

if [[  -e $EMAIL_REPORT ]]; then
 rm $EMAIL_REPORT
fi

if [[ $ZEKE_RC -ne 0 ]]; then
   echo " ZEKE Exit RC : " $ZEKE_RC
         exit 1
fi





###################### wrapper R ####################

#!/bin/ksh
#########################################################
# Wrapper Script for R Jobs
#
# Zeke will invoke this script by passing Zeke Job Name
# as a parameter.
#
#
#
#
#
#
#########################################################
#set -x

ZEKE_JOB_NAME=$1
#ROOT_PATH='/users/s056233/N807001/Framework_for_R1'
ROOT_PATH='/u01/datascience/common/bin'
TMP='/tmp'

ID=$RANDOM

user_id=$(whoami)
server_name=$(hostname)

ENV_FILE="$ROOT_PATH/common.env"
echo "ZEKE_JOB_NAME :" $ZEKE_JOB_NAME
echo "ENV_FILE : " $ENV_FILE 
echo $ROOT_PATH

if [[ -z $ZEKE_JOB_NAME ]]; then
  echo "Error: Missing zeke-job-nam parameter."
  echo "USAGE: wrapper_ds.ksh zeke-job-name "
  exit 1
fi

source $ENV_FILE
if [ $? -ne 0 ]; then
  echo "Error: Missing env file."
  exit 1
fi


sid=$user_id
echo "sid:" $sid
usid=$(echo $sid | tr '[:lower:]' '[:upper:]')
echo "usid : " $usid


/usr/bin/kinit $usid@AETH.AETNA.COM -k -t /users/$sid/$sid.keytab


date=`date -d"-1 days" +%Y-%m-%d`
yr=`echo $date | awk -F\- '{print $1}'`
mth=`echo $date | awk -F\- '{print $2}'`
dy=`echo $date | awk -F\- '{print $3}'`

PROGRESS="InProgress"
COMPLETE="Complete"

START_TIME=$(date  +%Y-%m-%d_%T)
echo "Start time: " $START_TIME

START_TIME_S=$SECONDS

PARAM_FILE=""$ROOT_PATH"/"$ZEKE_JOB_NAME-$START_TIME-$ID-PARAM".txt"
echo "Param file : " $PARAM_FILE


function send_email_report
{
awk -v ZEKE_JOB=$ZEKE_JOB_NAME -v ADDRESS=$EMAIL_TO -v STATUS="$1" '
{print $0 "<br>"}
BEGIN {
  print "To: " ADDRESS;
  print "Subject: " "Job " STATUS " Report for " ZEKE_JOB; 
  print "Content-Type: text/html";
  print "Content-Transfer-Encoding: 7bit";
  print "Content-Disposition: inline";
  print "Content-Base: \"http://www.aetna.com/\"";
  print "<html><head></head><body>"
}
END {print "<p><b>Do not reply to this message.  This is a service email account and is not monitored.</p></b></body></html>"}
' | sendmail -t $EMAIL_TO
}


function abort_wrapper_script
{

  cat $2 | tail -n $LINES | send_email_report "Job Failure" $REPORT_EMAIL_FAILURE "$1" 
  echo "Job $ZEKE_JOB_NAME ($SQ_JOB_NAME) failed."
  #exit 99
}


function report_job_run
{
     cat $EMAIL_REPORT | send_email_report "$1"
}

function execute_sql
{
  RESULT=`mysql --skip-column-names -h$HOST --port $PORT  -u$USER -p$PW -e "$1"`
  RC=$?

  eval $2=\""$RESULT"\"
  eval $3=\""$RC"\"
}

##########  Get the job attributes from the Job Master table ############
SQLSTMT="select j.job_name as R_job_name, a.email as email_to, a.script_path as script_path, a.log_path as log_path, COALESCE(nullif(j.job_path,''),'-'),j.tools_used as type, j.retry_attempt as retry,j.op_stats_table as stats_table, a.app_name as app_name, j.job_id as job_id, a.yarn_logs as yarn_logs_loc,a.app_id as app_id from $APP_TABLE as a join $JOB_TABLE as j on j.app_id=a.app_id  where j.Zeke_Job_Name='$ZEKE_JOB_NAME';"
echo "SQL statement: " $SQLSTMT
execute_sql "$SQLSTMT" SQLRESULT RC

 echo "RC: " $RC
  if [ $RC -ne 0 ]; then
    echo "inside"
    EMAIL_REPORT=""$TMP"/temp.txt"
    EMAIL_TO=$EMAIL_ADDR

    echo "Job $ZEKE_JOB_NAME failed to load parameters."
    echo -e "Message: Failed to load parameters from app & job tables." >> $EMAIL_REPORT
    report_job_run "Failed "

    echo "EMAIL_REPORT :" $EMAIL_REPORT
    echo "EMIL_TO : " $EMAIL_TO

    if [[  -e $EMAIL_REPORT ]]; then
     rm $EMAIL_REPORT
   fi

   exit 1
  fi


echo $SQLRESULT | read R_JOB_NAME EMAIL_TO SCRIPT_PATH LOG_PATH JOB_PATH TYPE RETRY STATS_TABLE APP_NAME JOB_ID YARN_LOGS_LOC APP_ID 
echo JOB_NAME is $R_JOB_NAME
echo EMAIL_TO is $EMAIL_TO
echo SCRIPT_PATH is $SCRIPT_PATH 
echo LOG_PATH is $LOG_PATH
echo JOB_PATH is $JOB_PATH
echo TYPE is $TYPE
echo RETRY is  $RETRY
echo STATS_TABLE is $STATS_TABLE
echo APP_NAME is $APP_NAME
echo JOB_ID is $JOB_ID
echo APP_ID is $APP_ID
echo PW is $PW | sed "s/$PW/*******/"

##########  Get the cycle  attributes from the Job Cycle table ############
SQLSTMT="select status,cycle_id from $CYCLE_TABLE where app_id='$APP_ID' order by cycle_id desc limit 1 ;"
execute_sql "$SQLSTMT" SQLRESULT RC
  echo "RC: " $RC
  if [ $RC -ne 0 ]; then
    echo "Eror retrieving status: "
  fi 

echo $SQLRESULT | read STATUS CYCLE_ID 
echo STATUS is $STATUS
echo CYCLE_ID is $CYCLE_ID
if [[ "$STATUS" != $PROGRESS ]]; then
	echo Error: Job is not scheduled !
        exit 1
fi

PARAMS_FILE=""$ROOT_PATH"/"$ZEKE_JOB_NAME-$START_TIME-$ID-PARAMS".txt"
echo "PARAMS file : " $PARAMS_FILE

#PARAMETER_FILE=""$SCRIPT_PATH"/"$R_JOB_NAME".txt"
PARAMETER_FILE=""$ROOT_PATH"/"$R_JOB_NAME".txt"
echo "PARAMETER file : " $PARAMETER_FILE

##########  Get the param  attributes from the Job Param table ############
SQLSTMT="select p.param_name,p.param_value from $PARAM_TABLE as p join $JOB_TABLE as j on p.job_id=j.job_id where j.Zeke_Job_Name='$ZEKE_JOB_NAME';"
echo "SQL statement: " $SQLSTMT
SQLRESULT=`mysql --skip-column-names -h$HOST --port $PORT  -u$USER -p$PW -e "$SQLSTMT"`
RC=$?
echo "RC: " $RC
if [ $RC -ne 0 ]; then
   echo "inside"
   EMAIL_REPORT=""$TMP"/temp.txt"
   EMAIL_TO=$EMAIL_ADDR

    echo "Job $ZEKE_JOB_NAME failed to load parameters."
    echo -e "Message: Failed to load parameters from param table." >> $EMAIL_REPORT
    report_job_run "Failed "

    echo "EMAIL_REPORT :" $EMAIL_REPORT
    echo "EMIL_TO : " $EMAIL_TO

    if [[  -e $EMAIL_REPORT ]]; then
        rm $EMAIL_REPORT
    fi

    exit 1
  fi


echo "$SQLRESULT" >> $PARAM_FILE


######### Construct parameter list #########
cat $PARAM_FILE | while read line
  do
   echo "line:" $line 
  F1=`echo $line | cut -f1 -d' '`
  echo $F1
  F2=`echo $line | cut -f2- -d' '`
  echo $F2 
  if [[ ! -z $F1 && ! -z $F2 ]]; then
     C1=`echo $F2 | cut -c1-1`
      if [[ "$C1" == '`' ]]; then
              eval F2VAL=$F2
          echo "F2VAL : " $F2VAL
          PARAMLIST=`printf $F1"=\""$F2VAL\"`
      else
          PARAMLIST=`printf $F1"=\""$F2\"`
    fi
    echo "$PARAMLIST" >> $PARAMS_FILE
  fi
  
 done

##### Remove the args prefis if ther are any integer or vector passed with args prefix in the param table######### 
cat $PARAMS_FILE | awk '!/args/' > $PARAMETER_FILE
cat $PARAMS_FILE | grep -i "args" |  sed 's/"//g' | cut -d'.' -f2 >> $PARAMETER_FILE


echo "PARAMLIST : " $PARAMLIST
echo "EMAIL_TO : " $EMAIL_TO
echo "TOOLS: " $TOOLS
echo "APP_NAME: " $APP_NAME
echo "JOB_ID: " $JOB_ID
echo  "LOG_PATH: " $LOG_PATH
  
JOB_TYPE=`echo $R_JOB_NAME | cut -f2- -d'.'`

if [ "$DEPLOY_MODE" == "AUTO" ]; then
   SCRIPTS=""
   LOGS=""
else
   SCRIPTS="/scripts"
   LOGS="/logs"
fi

if [ "$JOB_PATH" == "-" ]; then
   SCRIPT_DIR=${SCRIPT_PATH}
   LOG_DIR=${LOG_PATH}
else
   SCRIPT_DIR=${SCRIPT_PATH}/${JOB_PATH}${SCRIPTS}
   LOG_DIR=${LOG_PATH}/${JOB_PATH}${LOGS}  
fi    

if [ ! -d "$LOG_DIR" ]; then
   mkdir -p $LOG_DIR
   chmod -R 775 $LOG_DIR
fi

if [[ "$TYPE" == "R" ]]; then
     if [ "$JOB_TYPE" == "R" ]; then
         R_SCRIPT=${SCRIPT_DIR}/${R_JOB_NAME}
     else
         R_SCRIPT=${SCRIPT_DIR}/${R_JOB_NAME}.R
     fi
else
     echo "Exit: not r job !" 
     exit 1
fi
 
echo "R script name: " $R_SCRIPT
echo "Job_Name: " $R_JOB_NAME

######## Set the log directory path and the log file name. ##########
LOG_FILE=""$LOG_DIR"/"$R_JOB_NAME-$START_TIME-$ID".log"
echo "Log file : " $LOG_FILE

EMAIL_REPORT=""$TMP"/"$R_JOB_NAME-$START_TIME"-email-report-$ID.txt"
echo "Email report file: " $EMAIL_REPORT

SUMMARY_REPORT="$R_JOB_NAME-summary-report-$ID.txt"

DETAIL_REPORT="$R_JOB_NAME-detail-report-$ID.txt"



ZEKE_RC=0 

####### Get the server details from the parameter file passed in the param table and remove the server information from the parameter files 
SSH_SERVER=$(cat $PARAMETER_FILE | grep -i 'server' |cut -d'=' -f2 | sed 's/"//g')
sed -i '/server/d' $PARAMETER_FILE

##### SCP the parameter file to server ######
scp $PARAMETER_FILE $SSH_SERVER:$SCRIPT_PATH/

#### SSH the server in the which R script is located and execute the R script. Get the log information In the current server#####
ssh $SSH_SERVER $R $R_SCRIPT > $LOG_FILE 2>&1

RC=$?
echo $RC
ZEKE_RC=$RC
if [ $RC -eq 0 ]; then
  echo "Job $ZEKE_JOB_NAME ($R_JOB_NAME) successfully ran."
  JOB_STATUS="SUCCESS"
  elif [ $RC -ne 0 ]; then
       echo "TOTAL RETRIES: " $RETRY
       i=0
       while [ $RETRY -gt i ]
       do
         echo "Retry # : " $i
         ssh $SSH_SERVER $R $R_SCRIPT > $LOG_FILE 2>&1
         RC=$?
         if [ $RC -eq 0 ]; then
           break
         fi
         let i=i+1
       done
fi
 
#### Capture the information if the R script failed and generate yarn log and store in Hadoop #######
if [ $RC -ne 0 ]; then
		echo  "R  script $R_SCRIPT failed." 
          JOB_STATUS="Failed"
   
        
		if [ $(cat $LOG_FILE | grep -i "FAILED" | wc -w) -gt 0 ]; then
			message=$(cat $LOG_FILE | grep -i "FAILED"| cut -d":" -f2- |sed s"/:/-/g")
      echo "message: " $message         	
    fi
		 
		if [ $(cat $LOG_FILE | grep -i "ERROR" | wc -w) -gt 0 ]; then
			message=$(cat $LOG_FILE | grep -i "ERROR" | sed "s/'/ /g")
			echo "message: " $message 
		fi	

    if [ $(cat $LOG_FILE | grep -i "Submitted application" | wc -w) -gt 0 ]; then
          echo Submitted application
          application=$(cat $LOG_FILE | grep -i "Submitted application")
          echo "application : " $application
          app_id=$(echo $application | cut -d" " -f9)
          echo "app_id :  " $app_id

          YARN_FILE_NAME="$ZEKE_JOB_NAME-$ID-$app_id.log"
          YARN_LOG_FILE=""$LOG_PATH"/"$YARN_FILE_NAME""

          echo "sleeping for 180 sec"
          sleep 180

          yarn logs -applicationId $app_id >> $YARN_LOG_FILE
          RC=$?
          echo "Yarn RC : " $RC
          if [ $RC -ne 0 ]; then
             echo "Yarn retry:"
             sleep 180
             yarn logs -applicationId $app_id >> $YARN_LOG_FILE
          fi
           hadoop fs -put $YARN_LOG_FILE yarnlogs
     fi
fi		 

END_TIME=$(date +"%Y-%m-%d %T")
echo "Job Stats ended at "$END_TIME

END_TIME_S=$SECONDS
time_taken=$((END_TIME_S- START_TIME_S))
date="$(date +'%d-%m-%Y')"

echo "Message: " $message

	 
if [[ -z "$message" ]]; then
        echo -e "App Name: $APP_NAME\nJob Name: $R_JOB_NAME\nLog File: $LOG_FILE\nStatus: $JOB_STATUS\nTime Taken: $time_taken(sec)\nDate: $date" >> $EMAIL_REPORT
        report_job_run ""
   elif [[ -z "$app_id" ]]; then
        echo -e "App Name: $APP_NAME\nJob Name: $R_JOB_NAME\nLog File: $LOG_FILE\nStatus: $JOB_STATUS\nMessage: $message\nTime Taken: $time_taken(sec)\nDate: $date" >> $EMAIL_REPORT
        report_job_run "Failed "
   else
     echo -e "App Name: $APP_NAME\nJob Name: $R_JOB_NAME\nLog File: $LOG_FILE\nStatus: $JOB_STATUS\nMessage: $message\nTime Taken: $time_taken(sec)\nDate: $date\nYarn Log File: $YARN_FILE_NAME\nHDFS Loc: $YARN_LOGS_LOC\nAPP ID:$app_id" >> $EMAIL_REPORT
        report_job_run "Failed "
fi

job_id=$R_JOB_NAME

############Capture the stats from Log filr if any Hoive table has ben written by the script #########	
LOG=$LOG_FILE
if [[ "$STATS_TABLE" != 'NULL' ]]; then
arr=$(echo $STATS_TABLE | tr "," "\n")
echo "arr :" $arr
for tname in $arr
do
   table_name=$tname
    echo "table name: " $tname
     num_rows_loaded=0
     size=0
    time_taken=0
    if [ $(cat $LOG | grep -i "$tname stats" | wc -w) -gt 0 ]; then
      cat $LOG  | grep -i "$tname stats" > $tname.log

      rows=$(cat $tname.log | grep -o "numRows=.*" | cut -d',' -f1 | cut -d'=' -f2 |  sed 's/]//g') 
      size=$(cat $tname.log | grep -o "totalSize=.*" | cut -d',' -f1 | cut -d'=' -f2 | sed 's/]//g')    

       
       num_rows_loaded=${rows//[[:space:]]/}
       size=${size//[[:space:]]/}
       echo "rows: " $rows
       echo "size: " $size
       

       status="SUCCESS"
       line_num=$(grep -n "$tname stats" $LOG | cut -d: -f1)
       echo "line_num : " $line_num
       a=0
       let ln3=$line_num + $a
       pt=$(sed -n $(($ln3+2))p $LOG)
       echo "Processed_time: " $pt
       time_taken=$(echo $pt | cut -d: -f2)
       echo "time_taken: " $time_taken
     
       elif [ $(cat $ LOG| grep -i "FAILED" | wc -w) -gt 0 ]; then
               echo "FAILED"
             status="FAILED"
             num_rows_loaded=0
             message=$(cat $LOG | grep -i "FAILED")
             message=$(echo $message | cut -d":" -f2)
    fi
   echo "message: " $message
   echo "Detail Report: " $DETAIL_REPORT

   echo -e "$JOB_ID\t$tname\t$num_rows_loaded\t$size\t$time_taken\t$CYCLE_ID" >> $DETAIL_REPORT
    if [[  -e $tname.log ]]; then
      rm $tname.log
    fi
done
else   
        echo STATS TABLE is $STATS_TABLE
	echo -e "$JOB_ID\tNULL\t0\t0\t0\t$CYCLE_ID" >> $DETAIL_REPORT
	fi
echo "Detail Table :" $DETAIL_TABLE

mysql -h$HOST --port $PORT  -u$USER -p$PW  -e " LOAD DATA LOCAL INFILE \"${DETAIL_REPORT}\" INTO TABLE $DETAIL_TABLE fields terminated by '\t' lines terminated by '\n' "

RC=$?

echo "Detail table RC: " $RC



SQLSTMT="insert into $SUMMARY_TABLE values ($JOB_ID,'$R_JOB_NAME','$TOOLS','$user_id','$server_name','$START_TIME','$END_TIME','$message','$JOB_STATUS','$CYCLE_ID');"
echo "SQLSTMT : " $SQLSTMT

SQLRESULT=`mysql --skip-column-names -h$HOST --port $PORT  -u$USER -p$PW -e "$SQLSTMT"`

RC=$?
echo "SUMMARY TABLE RC :" $RC

if [[  -e $DETAIL_REPORT ]]; then
   rm $DETAIL_REPORT
fi

if [[  -e $PARAM_FILE ]]; then
   rm $PARAM_FILE
fi

if [[  -e $PARAMS_FILE ]]; then
  rm $PARAMS_FILE
fi

if [[  -e $EMAIL_REPORT ]]; then
 rm $EMAIL_REPORT
fi

if [[  -e $SUMMARY_REPORT ]]; then
 rm $SUMMARY_REPORT
fi

if [[  -e $YARN_LOG_FILE ]]; then
   rm $YARN_LOG_FILE
fi

if [[ $ZEKE_RC -ne 0 ]]; then
   echo " ZEKE Exit RC : " $ZEKE_RC
         exit 1
fi

####### wrapper shell ########

#!/bin/ksh
#########################################################
# Wrapper Script for Shell Jobs
# 
# Zeke will invoke this script by passing Zeke Job Name
# as a parameter.
# 
#########################################################
#set -x
ZEKE_JOB_NAME=$1
ROOT_PATH='/u01/datascience/common/bin'
. $ROOT_PATH/functions/commonutil.ksh

function Job_Report
{
                 echo -e "App Name: $APP_NAME\nJob Name: $SHELL_JOB_NAME\nBatch Name: $BATCH_NAME\nLog File: $LOG_FILE\nStatus: $1\nTime Taken: $time_taken(sec)\nDate: $date" >> $EMAIL_REPORT
                 report_job_run "Success"

}

TMP='/tmp'
ID=$RANDOM

user_id=$(whoami)
server_name=$(hostname)

ENV_FILE="$ROOT_PATH/common.env"
echo "ZEKE_JOB_NAME :" $ZEKE_JOB_NAME
echo "ENV_FILE : " $ENV_FILE 

if [[ -z $ZEKE_JOB_NAME ]]; then
  echo "Error: Missing zeke-job-nam parameter."
  echo "USAGE: wrapper_ds.ksh zeke-job-name "
  exit 1
fi

source $ENV_FILE
if [ $? -ne 0 ]; then
  echo "Error: Missing env file."
  exit 1
fi

sid=$user_id
echo "sid:" $sid
usid=$(echo $sid | tr '[:lower:]' '[:upper:]')
echo "usid : " $usid

/usr/bin/kinit $usid@AETH.AETNA.COM -k -t /users/$sid/$sid.keytab

date=`date -d"-1 days" +%Y-%m-%d`
yr=`echo $date | awk -F\- '{print $1}'`
mth=`echo $date | awk -F\- '{print $2}'`
dy=`echo $date | awk -F\- '{print $3}'`

PROGRESS="InProgress"
COMPLETE="Complete"

START_TIME=$(date  +%Y-%m-%d_%T)
START_TIME_S=$SECONDS

PARAM_LOG_FILE=""$TMP"/"$ZEKE_JOB_NAME-$START_TIME-$ID-PARAMS".log"
echo "Log file : " $PARAM_LOG_FILE

PARAM_FILE=""$TMP"/"$ZEKE_JOB_NAME-$START_TIME-$ID-PARAM".txt"
echo "Param file : " $PARAM_FILE

ERRORLIST_FILE=""$TMP"/"$ZEKE_JOB_NAME-$START_TIME-$ID-ERRORLIST".txt"
typeset -A error_msgs

##########  Get the job attributes from the Job Master table ############

SQLSTMT="select j.job_name, a.email , a.script_path , a.log_path ,COALESCE(nullif(j.job_path,''),'-') , j.tools_used , j.retry_attempt ,coalesce(nullif(j.op_stats_table,''),'NULL') , a.app_name , j.job_id , a.yarn_logs ,a.app_id, j.batchid, j.batchname  from $APP_TABLE as a join $JOB_TABLE as j on j.app_id=a.app_id  where j.Zeke_Job_Name='$ZEKE_JOB_NAME';"

echo "SQL statement: " $SQLSTMT
execute_sql "$SQLSTMT" SQLRESULT RC
  echo "RC: " $RC
  if [ $RC -ne 0 ]; then
    echo "inside" 
    EMAIL_REPORT=""$TMP"/temp.txt"
    EMAIL_TO=$EMAIL_ADDR

    echo "Job $ZEKE_JOB_NAME failed to load parameters."
    echo -e "Message: Failed to load parameters from app & job tables." >> $EMAIL_REPORT
    report_job_run "Failed "

    echo "EMAIL_REPORT :" $EMAIL_REPORT
    echo "EMIL_TO : " $EMAIL_TO

    if [[  -e $EMAIL_REPORT ]]; then
     rm $EMAIL_REPORT
    fi

    exit 1 
  fi
echo $SQLRESULT | read SHELL_JOB_NAME EMAIL_TO SCRIPT_PATH LOG_PATH JOB_PATH TYPE RETRY STATS_TABLE APP_NAME JOB_ID YARN_LOGS_LOC APP_ID BATCH_ID BATCH_NAME
echo JOB_NAME is $SHELL_JOB_NAME
echo EMAIL_TO is $EMAIL_TO
echo SCRIPT_PATH is $SCRIPT_PATH
echo LOG_PATH is $LOG_PATH
echo JOB_PATH is $JOB_PATH
echo TYPE is $TYPE
echo RETRY is  $RETRY
echo STATS_TABLE is $STATS_TABLE
echo APP_NAME is $APP_NAME
echo JOB_ID is $JOB_ID
echo YARN_LOGS_LOC is $YARN_LOGS_LOC
echo APP_ID is $APP_ID
echo BATCH_ID is $BATCH_ID
echo BATCH_NAME is $BATCH_NAME


##########  Get the cycle  attributes from the Job Cycle table ############
SQLSTMT="select status,cycle_id,app_id,batchid from $CYCLE_TABLE where app_id='$APP_ID'and batchid='$BATCH_ID' and status='InProgress' order by cycle_id desc limit 1 ;"

echo SQLSTMT is $SQLSTMT
execute_sql "$SQLSTMT" SQLRESULT RC
  echo "RC: " $RC
  if [ $RC -ne 0 ]; then
    echo "Eror retrieving status: "
  fi 

echo $SQLRESULT | read STATUS CYCLE_ID APP_ID B_ID
echo STATUS is $STATUS
echo CYCLE_ID is $CYCLE_ID
echo APP_ID is $APP_ID
echo CYCLE BATCH_ID is $B_ID

if [[ "$STATUS" != $PROGRESS ]]; then
	echo Error: Cycle is not in progress, Please open the cycle and rerun !
        exit 1
fi

JOB_TYPE=`echo $SHELL_JOB_NAME | cut -f2- -d'.'`

if [ "$DEPLOY_MODE" == "AUTO" ]; then
   SCRIPTS=""
   LOGS=""
else
   SCRIPTS="/scripts"
   LOGS="/logs"
fi

if [ "$JOB_PATH" == "-" ]; then
   SCRIPT_DIR=${SCRIPT_PATH}
   LOG_DIR=${LOG_PATH}
else
   SCRIPT_DIR=${SCRIPT_PATH}/${JOB_PATH}${SCRIPTS}
   LOG_DIR=${LOG_PATH}/${JOB_PATH}${LOGS}  
fi  

if [ ! -d "$LOG_DIR" ]; then
   mkdir -p $LOG_DIR
   chmod -R 775 $LOG_DIR
fi   

if [[ "$TYPE" == "shell" ]]; then
     if [ "$JOB_TYPE" == "sh" ]; then
         SHELL_FILE=${SCRIPT_DIR}/${SHELL_JOB_NAME}
     else
         SHELL_FILE=${SCRIPT_DIR}/${SHELL_JOB_NAME}.sh
     fi
else
     echo "Exit: not shell job !" 
     exit 1
fi

#########  Get the param  attributes from the Job Param table ############
SQLSTMT="select p.param_name,p.param_value from $PARAM_TABLE as p join $JOB_TABLE as j on p.job_id=j.job_id where j.Zeke_Job_Name='$ZEKE_JOB_NAME';"
echo "SQL statement: " $SQLSTMT
SQLRESULT=`mysql --skip-column-names -h$HOST --port $PORT  -u$USER -p$PW -e "$SQLSTMT"`
RC=$?
echo "RC: " $RC
if [ $RC -ne 0 ]; then
    echo "inside"
    EMAIL_REPORT=""$TMP"/temp.txt"
    EMAIL_TO=$EMAIL_ADDR

    echo "Job $ZEKE_JOB_NAME failed to load parameters."
    echo -e "Message: Failed to load parameters from param table." >> $EMAIL_REPORT
    report_job_run "Failed "

    echo "EMAIL_REPORT :" $EMAIL_REPORT
    echo "EMIL_TO : " $EMAIL_TO

    if [[  -e $EMAIL_REPORT ]]; then
     rm $EMAIL_REPORT
    fi

    exit 1
  fi

echo "$SQLRESULT" >> $PARAM_FILE


######### Construct parameter list #########
cat $PARAM_FILE | while read line
 do
  echo "line:" $line
  F1=`echo $line | cut -f1 -d' '`
  F2=`echo $line | cut -f2- -d' '`
  if [[ ! -z $F1 && ! -z $F2 ]]; then
     C1=`echo $F2 | cut -c1-1`
      if [[ "$C1" == '`' ]]; then
         eval F2VAL=$F2
          echo "F2VAL : " $F2VAL
         PARAMLIST=$(echo $PARAMLIST $F2VAL)
      else
         PARAMLIST=$(echo $PARAMLIST $F2)
    fi
  fi
 done
 echo PARAMLIST is $PARAMLIST

echo "Shell File Name: " $SHELL_FILE
echo "Job_Name: " $SHELL_JOB_NAME
echo "Param List :" $PARAMLIST

######## Set the log directory path and the log file name. ##########
#LOG_FILE=""$LOG_PATH"/"$SHELL_JOB_NAME-$START_TIME-$ID".log"

LOG_FILE=""$LOG_DIR"/"$SHELL_JOB_NAME-$START_TIME-$ID".log"
echo "Log file : " $LOG_FILE

EMAIL_REPORT=""$TMP"/"$SHELL_JOB_NAME-$START_TIME"-email-report-$ID.txt"
echo "Email report file: " $EMAIL_REPORT


SHELL_INPUT=""$SHELL_STANDARD_PARAMS" "$SHELL_FILE" "$PARAMLIST""
echo "Shell String: " $SHELL_INPUT

job_start_time=$(date +%Y-%m-%d_%T)
$SH $SHELL_INPUT >> $LOG_FILE 2>&1

RC=$?
echo SHELL RC: $RC
RCA=0

if [ $RC -ne 0 ]; then
       echo "TOTAL RETRIES: " $RETRY
       i=0
       while [ $RETRY -gt i ]
       do
        echo "Retry # : " $i
       $SH $SHELL_INPUT >> $LOG_FILE-$i 2>&1
        RCA=$?
        if [ $RCA -eq 0 ]; then
          break
        fi
        let i=i+1
       done
fi

END_TIME=$(date +"%Y-%m-%d %T")
        echo "Job Stats ended at "$END_TIME
        END_TIME_S=$SECONDS
        time_taken=$((END_TIME_S- START_TIME_S))
        date="$(date +'%d-%m-%Y')"

echo RC is  $RC
ZEKE_RC=$RC	

 if [ $RC -ne 0 ] || [ $RCA -ne 0 ]; then
    echo "Eror While Executing Shell Job, Please Retry !! "
           Job_Report "Failure"
  else
       echo "Sending success notification: "
           Job_Report "Success"
  fi


if [[  -e $EMAIL_REPORT ]]; then
 rm $EMAIL_REPORT
fi

if [[  -e $PARAM_FILE ]]; then
   rm $PARAM_FILE
fi

if [[  -e $PARAM_LOG_FILE ]]; then
   rm $PARAM_LOG_FILE
fi

if [[ $ZEKE_RC -ne 0 ]]; then
   echo " ZEKE Exit RC : " $ZEKE_RC
   exit 1
fi


####################### wrappper spark ##########################

#!/bin/ksh
#########################################################
# Wrapper Script for Spark Jobs
# 
# Zeke will invoke this script by passing Zeke Job Name
# as a parameter.
# 
# 
# 
# 
#
# Revision History
# 0.1 11/19/2015 Initial draft.
#
#########################################################
#set -x
ZEKE_JOB_NAME=$1
ROOT_PATH='/u01/datascience/common/bin'
TMP='/tmp'
ID=$RANDOM

. $ROOT_PATH/lib/commonutil.ksh
. $ROOT_PATH/lib/sparkutil.ksh

user_id=$(whoami)
server_name=$(hostname)

SPARK_PATH='/usr/hdp/current/spark-client/bin'
ENV_FILE="$ROOT_PATH/spark.env"
echo "ZEKE_JOB_NAME :" $ZEKE_JOB_NAME
echo "ENV_FILE : " $ENV_FILE 

if [[ -z $ZEKE_JOB_NAME ]]; then
  echo "Error: Missing zeke-job-nam parameter."
  echo "USAGE: wrapper_ds.ksh zeke-job-name "
  exit 1
fi

source $ENV_FILE
if [ $? -ne 0 ]; then
  echo "Error: Missing env file."
  exit 1
fi

sid=$user_id
echo "sid:" $sid
usid=$(echo $sid | tr '[:lower:]' '[:upper:]')
echo "usid : " $usid

/usr/bin/kinit $usid@AETH.AETNA.COM -k -t /users/$sid/$sid.keytab

date=`date -d"-1 days" +%Y-%m-%d`
yr=`echo $date | awk -F\- '{print $1}'`
mth=`echo $date | awk -F\- '{print $2}'`
dy=`echo $date | awk -F\- '{print $3}'`

PROGRESS="InProgress"
COMPLETE="Complete"

START_TIME=$(date  +%Y-%m-%d_%T)
START_TIME_S=$SECONDS

PARAM_LOG_FILE=""$TMP"/"$ZEKE_JOB_NAME-$START_TIME-$ID-PARAMS".log"
echo "Log file : " $PARAM_LOG_FILE

PARAM_FILE=""$TMP"/"$SPARK_JOB_NAME-$START_TIME-$ID-PARAM".txt"
echo "Param file : " $PARAM_FILE
count=0


##########  Get the job attributes from the Job Master table ############
SQLSTMT="select j.job_name as hive_job_name, a.email as email_to, a.script_path as script_path, a.log_path as log_path, COALESCE(nullif(j.job_path,''),'-') ,j.tools_used as type, j.retry_attempt as retry,coalesce(nullif(j.op_stats_table,''),'NULL') as stats_table, a.app_name as app_name, j.job_id as job_id, a.yarn_logs as yarn_logs_loc,a.app_id as app_id from $APP_TABLE as a join $JOB_TABLE as j on j.app_id=a.app_id  where j.Zeke_Job_Name='$ZEKE_JOB_NAME';"
echo "SQL statement: " $SQLSTMT
execute_sql "$SQLSTMT" SQLRESULT RC
  echo "RC: " $RC
  if [ $RC -ne 0 ]; then
    echo "inside" 
    EMAIL_REPORT=""$TMP"/temp.txt"
    EMAIL_TO=$EMAIL_ADDR

    echo "Job $ZEKE_JOB_NAME failed to load parameters."
    echo -e "Message: Failed to load parameters from app & job tables." >> $EMAIL_REPORT
    report_job_run "Failed "

    echo "EMAIL_REPORT :" $EMAIL_REPORT
    echo "EMIL_TO : " $EMAIL_TO

    if [[  -e $EMAIL_REPORT ]]; then
     rm $EMAIL_REPORT
    fi

    exit 1 
  fi
echo $SQLRESULT | read SPARK_JOB_NAME EMAIL_TO SCRIPT_PATH LOG_PATH JOB_PATH TYPE RETRY STATS_TABLE APP_NAME JOB_ID YARN_LOGS_LOC APP_ID
echo JOB_NAME is $SPARK_JOB_NAME
echo EMAIL_TO is $EMAIL_TO
echo SCRIPT_PATH is $SCRIPT_PATH
echo LOG_PATH is $LOG_PATH
echo JOB_PATH is $JOB_PATH
echo TYPE is $TYPE
echo RETRY is  $RETRY
echo STATS_TABLE is $STATS_TABLE
echo APP_NAME is $APP_NAME
echo JOB_ID is $JOB_ID
echo YARN_LOGS_LOC is $YARN_LOGS_LOC
echo APP_ID is $APP_ID


##########  Get the cycle  attributes from the Job Cycle table ############
SQLSTMT="select status,cycle_id from $CYCLE_TABLE where app_id='$APP_ID' order by cycle_id desc limit 1 ;"
execute_sql "$SQLSTMT" SQLRESULT RC
  echo "RC: " $RC
  if [ $RC -ne 0 ]; then
    echo "Eror retrieving status: "
  fi 

echo $SQLRESULT | read STATUS CYCLE_ID
echo STATUS is $STATUS
echo CYCLE_ID is $CYCLE_ID
if [[ "$STATUS" != $PROGRESS ]]; then
	echo Error: Cycle is not in progress, Please open the cycle and rerun !
        exit 1
fi

##########  Get the param  attributes from the Job Param table ############
SQLSTMT="select p.param_name,p.param_value from $PARAM_TABLE as p join $JOB_TABLE as j on p.job_id=j.job_id where j.Zeke_Job_Name='$ZEKE_JOB_NAME';"
echo "SQL statement: " $SQLSTMT
SQLRESULT=`mysql --skip-column-names -h$HOST --port $PORT  -u$USER -p$PW -e "$SQLSTMT"`
RC=$?
echo "RC: " $RC
if [ $RC -ne 0 ]; then
    echo "inside" 
    EMAIL_REPORT=""$TMP"/temp.txt"
    EMAIL_TO=$EMAIL_ADDR

    echo "Job $ZEKE_JOB_NAME failed to load parameters."
    echo -e "Message: Failed to load parameters from param table." >> $EMAIL_REPORT
    report_job_run "Failed "

    echo "EMAIL_REPORT :" $EMAIL_REPORT
    echo "EMIL_TO : " $EMAIL_TO

    if [[  -e $EMAIL_REPORT ]]; then
     rm $EMAIL_REPORT
    fi

    exit 1 
  fi

echo "$SQLRESULT" >> $PARAM_FILE

######### Construct parameter list #########
cat $PARAM_FILE | while read line
 do
  echo "line:" $line
  NUM_WC=`echo $line | wc -w`
  if [[ $NUM_WC -gt 1 ]]; then
	  F1=`echo $line | cut -f1 -d' '`
	  F2=`echo $line | cut -f2- -d' '`
	  if [[ ! -z $F1 && ! -z $F2 ]]; then
	     C1=`echo $F2 | cut -c1-1`
	      if [[ "$C1" == '`' ]]; then
		 eval F2VAL=$F2
		  echo "F2VAL : " $F2VAL
		 PARAMLIST=$(echo $PARAMLIST $F1 $F2VAL)
	      else
		 PARAMLIST=$(echo $PARAMLIST $F1 $F2)
	    fi
	  fi
   elif [[ $NUM_WC == 1 ]];then
     echo NUM_WC is : $NUM_WC
     F1=`echo $line | cut -f1 -d' '`
     PARAMLIST=$(echo $PARAMLIST $F1)
  fi 
 done 
 echo PARAMLIST is $PARAMLIST
 
if [ "$DEPLOY_MODE" == "AUTO" ]; then
   LOGS=""
else
   LOGS="/logs"
fi

if [ "$JOB_PATH" == "-" ]; then
   LOG_DIR=${LOG_PATH}
else
   LOG_DIR=${LOG_PATH}/${JOB_PATH}${LOGS}  
fi 

if [ ! -d "$LOG_DIR" ]; then
   mkdir -p $LOG_DIR
   chmod -R 775 $LOG_DIR
fi
 
echo "Job_Name: " $SPARK_JOB_NAME
echo "Param List :" $PARAMLIST

######## Set the log directory path and the log file name. ##########
#LOG_FILE=""$LOG_PATH"/"$SPARK_JOB_NAME-$START_TIME-$ID".log"

LOG_FILE=""$LOG_DIR"/"$SPARK_JOB_NAME-$START_TIME-$ID".log"
echo "Log file : " $LOG_FILE

EMAIL_REPORT=""$TMP"/"$SPARK_JOB_NAME-$START_TIME"-email-report-$ID.txt"
echo "Email report file: " $EMAIL_REPORT

DETAIL_REPORT=""$TMP"/"$SPARK_JOB_NAME"-detail-report-"$ID".txt"

job_start_time=$(date +%Y-%m-%d_%T)
$SPARK_PATH/spark-submit $PARAMLIST >> $LOG_FILE 2>&1
#$SPARK_PATH/spark-submit $PARAMLIST
RC=$?
echo $RC
ZEKE_RC=$RC

if [ $RC -ne 0 ]; then
       echo "TOTAL RETRIES: " $RETRY
       i=0
       while [ $RETRY -gt i ]
       do
         echo "Retry # : " $i 
         $SPARK_PATH/spark-submit $PARAMLIST >> $LOG_FILE-$i 2>&1
         RC=$?
         if [ $RC -eq 0 ]; then
           break
         fi
         let i=i+1
       done
fi

extract_spark_log $APP_ID $ZEKE_JOB_NAME $START_TIME $ID $RC $TMP

if [[  -e $DETAIL_REPORT ]]; then
   rm $DETAIL_REPORT
fi

if [[  -e $PARAM_FILE ]]; then
   rm $PARAM_FILE
fi

if [[  -e $PARAM_LOG_FILE ]]; then
   rm $PARAM_LOG_FILE
fi

if [[  -e $EMAIL_REPORT ]]; then
 rm $EMAIL_REPORT
fi

if [[ $ZEKE_RC -ne 0 ]]; then
   echo " ZEKE Exit RC : " $ZEKE_RC
         exit 1
fi

  




############## wrapper sqoop #########
#!/bin/ksh

#########################################################
# Wrapper Script for Sqoop Jobs
# 
# Zeke will invoke this script by passing Zeke Job Name
# as a parameter.
# 
# 
# 
# 
#
# Revision History
# 0.1 10/06/2015 Initial draft.
#
#########################################################

#set -x
ZEKE_JOB_NAME=$1
ROOT_PATH='/u01/datascience/common/bin'
. $ROOT_PATH/functions/commonutil.ksh

SQOOP_PATH=`cat $ROOT_PATH/common.env | grep -w 'SQOOP' | cut -d'=' -f2`
#SQOOP_PATH='/var/webeng/hadoop/sqoop_hdp2/bin'
TMP='/tmp'
ID=$RANDOM

user_id=$(whoami)
server_name=$(hostname)

ENV_FILE="$ROOT_PATH/common.env"
echo "ZEKE_JOB_NAME :" $ZEKE_JOB_NAME
echo "ENV_FILE : " $ENV_FILE 

if [[ -z $ZEKE_JOB_NAME ]]; then
  echo "Error: Missing zeke-job-nam parameter."
  echo "USAGE: wrapper_ds.ksh zeke-job-name "
  exit 1
fi


source $ENV_FILE
if [ $? -ne 0 ]; then
  echo "Error: Missing env file."
  exit 1
fi

sid=$user_id
echo "sid:" $sid
usid=$(echo $sid | tr '[:lower:]' '[:upper:]')
echo "usid : " $usid

/usr/bin/kinit $usid@AETH.AETNA.COM -k -t /users/$sid/$sid.keytab

date=`date -d"-1 days" +%Y-%m-%d`
yr=`echo $date | awk -F\- '{print $1}'`
mth=`echo $date | awk -F\- '{print $2}'`
dy=`echo $date | awk -F\- '{print $3}'`

PROGRESS="InProgress"
COMPLETE="Complete"

START_TIME=$(date  +%Y-%m-%d_%T)
START_TIME_S=$SECONDS

PARAM_LOG_FILE=""$TMP"/"$ZEKE_JOB_NAME-$START_TIME-$ID-PARAMS".log"
#echo "Log file : " $PARAM_LOG_FILE
PARAM_FILE=""$TMP"/"$ZEKE_JOB_NAME-$START_TIME-$ID-PARAM".txt"
#echo "Param file : " $PARAM_FILE


size=0
count=0
tname=""
message=""

function paramlist_order {
   typeset -n arr=$1
   for p in "${arr[@]}";do
	if [[ ! -z ${params_hash["$p"]} ]] ; then
	    F1=$p
	    F2=${params_hash["$p"]}
            C1=`echo $F2 | cut -c1-1`
	    if [[ "$F2" == "NULL" ]]; then
		PARAMLIST=$(echo $PARAMLIST $F1)
	      elif [[ "$C1" == '`' ]]; then
		 eval F2VAL=$F2
		 # echo "F2VAL : " $F2VAL
		 PARAMLIST=$(echo $PARAMLIST $F1 $F2VAL)
		 elif	[[ ${F1:1:1} == "D" ]]; then
		 		PARAMLIST=$(echo $PARAMLIST $F1=$F2)
	      else
		 PARAMLIST=$(echo $PARAMLIST $F1 $F2)
	    fi
	    if [[ "$F1" == "--table" ]]; then
		 tname=$F2
	    fi
	   
	    if [[ "$F1" == '--connect' ]]; then
		CONNECT=$F2
	    fi
	    
	    if [[ "$F1" == '--username' ]]; then
		NZ_USER=$F2
	    fi
	    
	    if [[ "$F1" == '--password' ]]; then
		NZ_PW=$F2
	    fi

	   #PARAMLIST=$(echo $PARAMLIST "$p" ${params_hash["$p"]})
	   unset params_hash["$p"]
   	fi 
   done
}

set -A params
###### contains(string, substring)
typeset -A cust_queries
typeset -A params_hash

contains(){
	string="$1"
	substring="$2"
        echo String: $string
	echo Substring: $substring

	if test "${string#*$substring}" != "$string"
	then 
		return 0 
	else
		return 1
	fi

}
##########  Get the job attributes from the Job Master table ############
#SQLSTMT="select j.job_name as sq_job_name, a.email as email_to, a.script_path as script_path, a.log_path as log_path, j.tools_used as type, j.retry_attempt as retry, a.app_name as app_name, j.job_id as job_id, a.yarn_logs as yarn_logs_loc, a.app_id as app_id from $APP_TABLE as a join $JOB_TABLE as j on j.app_id=a.app_id  where j.Zeke_Job_Name='$ZEKE_JOB_NAME';"
SQLSTMT="select j.job_name , a.email , a.script_path , a.log_path , COALESCE(nullif(j.job_path,''),'-') , j.tools_used , j.retry_attempt , a.app_name , j.job_id , a.yarn_logs ,a.app_id, j.batchid, j.batchname  from $APP_TABLE as a join $JOB_TABLE as j on j.app_id=a.app_id  where j.Zeke_Job_Name='$ZEKE_JOB_NAME';"

echo "SQL statement: " $SQLSTMT
execute_sql "$SQLSTMT" SQLRESULT RC
echo "RC: " $RC
if [ $RC -ne 0 ]; then
    echo "inside" 
    EMAIL_REPORT=""$TMP"/temp.txt"
    EMAIL_TO=$EMAIL_ADDR

    echo "Job $ZEKE_JOB_NAME failed to load parameters."
    echo -e "Message: Failed to load parameters from app & job tables." >> $EMAIL_REPORT
    report_job_run "Failed "

    echo "EMAIL_REPORT :" $EMAIL_REPORT
    echo "EMIL_TO : " $EMAIL_TO

    if [[  -e $EMAIL_REPORT ]]; then
     rm $EMAIL_REPORT
    fi

    exit 1 
fi
echo $SQLRESULT | read SQ_JOB_NAME EMAIL_TO SCRIPT_PATH LOG_PATH JOB_PATH TYPE RETRY  APP_NAME JOB_ID YARN_LOGS_LOC APP_ID BATCH_ID BATCH_NAME
echo SQOOP JOB_NAME is $SQ_JOB_NAME
echo EMAIL_TO is $EMAIL_TO
echo SCRIPT_PATH is $SCRIPT_PATH
echo LOG_PATH is $LOG_PATH
echo JOB_PATH is $JOB_PATH
echo TYPE is $TYPE
echo RETRY is  $RETRY
echo APP_NAME is $APP_NAME
echo JOB_ID is $JOB_ID
echo YARN_LOGS_LOC is $YARN_LOGS_LOC
echo APP_ID is $APP_ID
echo BATCH_ID is $BATCH_ID
echo BATCH_NAME is $BATCH_NAME

##########  Get the cycle  attributes from the Job Cycle table ############
SQLSTMT="select status,cycle_id,app_id,batchid from $CYCLE_TABLE where app_id='$APP_ID'and batchid='$BATCH_ID' and status='InProgress' order by cycle_id desc limit 1 ;"

echo SQLSTMT is $SQLSTMT
execute_sql "$SQLSTMT" SQLRESULT RC
  echo "RC: " $RC
  if [ $RC -ne 0 ]; then
    echo "Eror retrieving status: "
  fi 

echo $SQLRESULT | read STATUS CYCLE_ID APP_ID B_ID
echo STATUS is $STATUS
echo CYCLE_ID is $CYCLE_ID
echo APP_ID is $APP_ID
echo CYCLE BATCH_ID is $B_ID

if [[ "$STATUS" != $PROGRESS ]]; then
	echo Error: Cycle is not in progress, Please open the cycle and rerun !
        exit 1
fi
   
##########  Get the param  attributes from the Job Param table ############

SQLSTMT="select p.param_name,coalesce(nullif(p.param_value,''),'NULL') from $PARAM_TABLE as p join $JOB_TABLE as j on p.job_id=j.job_id where j.Zeke_Job_Name='$ZEKE_JOB_NAME';"

echo "SQL statement: " $SQLSTMT
SQLRESULT=`mysql --skip-column-names -h$HOST --port $PORT  -u$USER -p$PW -e "$SQLSTMT"`
RC=$?
echo "RC: " $RC
if [ $RC -ne 0 ]; then
    EMAIL_REPORT=""$TMP"/temp.txt"
    EMAIL_TO=$EMAIL_ADDR

    echo "Job $ZEKE_JOB_NAME failed to load parameters."
    echo -e "Message: Failed to load parameters from param table." >> $EMAIL_REPORT
    report_job_run "Failed "

    echo "EMAIL_REPORT :" $EMAIL_REPORT
    echo "EMIL_TO : " $EMAIL_TO

    if [[  -e $EMAIL_REPORT ]]; then
     rm $EMAIL_REPORT
    fi

    exit 1 
  fi

echo "$SQLRESULT" >> $PARAM_FILE
######### Construct parameter list #########
cat $PARAM_FILE | while read line
 do
 echo "line:" $line
  F1=`echo $line | cut -f1 -d' '`
  F2=`echo $line | cut -f2- -d' '`
 # echo F1 $F1
  set -f
  if [[ "$F1" == '--password' ]]; then
    C1=`echo $F2 | cut -c1-1`
    if [[ "$C1" == '`' ]]; then
         eval F2VAL=$F2
         F2=$F2VAL 
    fi
  fi
  params_hash[$F1]=$F2

done
set +f
params[0]=-Dmapreduce.job.queuename
params[1]=-Dmapreduce.job.maxtaskfailures.per.tracker
params[2]=--connect
params[3]=--username
params[4]=--password
params[5]=--input-null-non-string
params[6]=--input-null-string
params[7]=--export-dir
params[8]=--direct
params[9]=--table

params[10]=-m
params[11]=--fields-terminated-by
params[12]=--lines-terminated-by

params[13]=--input-null-string
params[14]=--input-null-non-string
params[15]=--verbose
params[16]=--
params[17]=--max-errors
params[18]=--input-fields-terminated-by
params[19]=--relaxed-isolation
paramlist_order params 

for index in "${!params_hash[@]}"; do
 C1=`echo $index | cut -c1-1`
 if [[ "$C1" == '-' ]]; then
	PARAMLIST=$(echo $PARAMLIST "$index" ${params_hash["$index"]})
	unset params_hash["$index"]
 fi
done

for index in "${!params_hash[@]}"; do
  F1=$index
  F2=${params_hash["$index"]}
  contains "$F1" "custom-query" 
  RC=$?
  echo RC $RC

 if [[ ! -z $F1 && ! -z $F2 && $RC == 0 ]]; then
   #echo F1 $F1
   #echo F2 $F2
   cust_queries[$F1]=$F2
  else
    if [[ "$F1" == 'type' ]]; then
     	TYPE2=$F2
    fi
 fi
done

echo PARAMLIST is $PARAMLIST| sed "s/$NZ_PW/*******/"

echo NZ_USER is $NZ_USER
echo tname is $tname
echo CONNECT is $CONNECT
echo TYPE is $TYPE
echo TYPE2 is $TYPE2

if [[ -z $TYPE2 ]]; then
   echo -e "Error: Missing 'type' param.." >> $EMAIL_REPORT
   report_job_run "Failed"
   report_job_run "Failed"
   exit 1
fi

 if [[ -z "$SQ_JOB_NAME" ]]; then
      echo "Error: Job name empty!"
      exit 1
 fi

echo "Job_Name: " $SQ_JOB_NAME

if [ "$DEPLOY_MODE" == "AUTO" ]; then
   LOGS=""
else
   LOGS="/logs"
fi

if [ "$JOB_PATH" == "-" ]; then
   LOG_DIR=${LOG_PATH}
else
   LOG_DIR=${LOG_PATH}/${JOB_PATH}${LOGS}  
fi 

if [ ! -d "$LOG_DIR" ]; then
   mkdir -p $LOG_DIR
   chmod -R 775 $LOG_DIR
fi

######## Set the log directory path and the log file name. ##########
#if [ "$JOB_PATH" == "-" ]
#then
#LOG_FILE=""$LOG_PATH"/"$SQ_JOB_NAME-$START_TIME-$ID".log"
#else
#LOG_FILE=""$LOG_PATH"/"${JOB_PATH}"/logs/"$SQ_JOB_NAME-$START_TIME-$ID".log"
#fi
LOG_FILE=""$LOG_DIR"/"$SQ_JOB_NAME-$START_TIME-$ID".log"
echo "Log file : " $LOG_FILE

EMAIL_REPORT=""$TMP"/"$SQ_JOB_NAME-$START_TIME"-email-report-$ID.txt"
echo "Email report file: " $EMAIL_REPORT

ERRORLIST_FILE=""$TMP"/"$SQ_JOB_NAME-$START_TIME-$ID-ERRORLIST".txt"
typeset -A error_msgs

#########  Get  error list from the error  Master table ############
SQLSTMT="select script_type, error from $ERROR_TABLE where script_type='sqoop'";
echo "SQL statement: " $SQLSTMT
SQLRESULT=`mysql --skip-column-names -h$HOST --port $PORT  -u$USER -p$PW -e "$SQLSTMT"`
RC=$?
echo "RC: " $RC
if [ $RC -ne 0 ]; then
    echo "inside"
    EMAIL_REPORT=""$TMP"/temp.txt"
    EMAIL_TO=$EMAIL_ADDR

    echo "Job $ZEKE_JOB_NAME failed to load error list."
    echo -e "Message: Failed to load parameters from error table." >> $EMAIL_REPORT
    report_job_run "Failed "

    echo "EMAIL_REPORT :" $EMAIL_REPORT
    echo "EMIL_TO : " $EMAIL_TO

    if [[  -e $EMAIL_REPORT ]]; then
     rm $EMAIL_REPORT
    fi

    exit 1
fi
echo "$SQLRESULT" >> $ERRORLIST_FILE
k=0
######### Construct error list #########
cat $ERRORLIST_FILE | while read line
 do
  k=`expr $k + 1`
  script_type=`echo $line | cut -f1 -d' '`
  error=`echo $line | cut -f2- -d' '`
  if [[ "$script_type" == 'sqoop' ]]; then
        echo script type is sqoop....
        error_msgs[$k]=$error
  fi
done


if [[ "$TYPE2" == 'query' ]]; then
        echo TYPE2 is $TYPE2
        let a=0
        let b=1
        while [[ $a -lt ${#cust_queries[*]} ]] ; do
                query=${cust_queries["custom-query-$b"]}
                echo QUERY is $query
                $SQOOP_PATH/sqoop eval --connect  $CONNECT --username $NZ_USER --password $NZ_PW --query "$query" >> $LOG_FILE 2>&1
                RC=$?
                if [ $RC -ne 0 ]; then
                    ZEKE_RC=$RC
                    break
                fi
                let a+=1
                let b+=1
        done
        END_TIME_S=$SECONDS
        time_taken=$((END_TIME_S- START_TIME_S))
        if [ $RC == 0 ]
        then
            JOB_STATUS="Success"
            message="Successfully executed queries.."
            echo -e "App Name: $APP_NAME\nJob Name: $SQ_JOB_NAME\nBatch Name: $BATCH_NAME\nLog File: $LOG_FILE\nStatus: $JOB_STATUS\nMessage: $message\nTime Taken: $time_taken(sec)\nDate: $date\n" >> $EMAIL_REPORT
            report_job_run "Success"
        else
            echo -e "Failed to  execute query $query" >> $EMAIL_REPORT
        fi

fi

unset cust_queries

if [[ ! -z $TYPE && "$TYPE" == "sqoop" && "$TYPE2" == "export" ]]; then
    echo TYPE2 is  $TYPE2
    $SQOOP_PATH/sqoop export $PARAMLIST >> $LOG_FILE 2>&1
    RC=$?
    echo "RC : " $RC
    ZEKE_RC=$RC
    END_TIME_S=$SECONDS
    time_taken=$((END_TIME_S- START_TIME_S))
    if [ $RC == 0 ]; then
        JOB_STATUS="Success"
        if [ $(cat $LOG_FILE | grep -i "Exported" | wc -w) -gt 0 ]; then
    	    exported=$(cat $LOG_FILE | grep -i "Exported")
   	    echo "count : " $exported
   	    count=$(echo $exported | cut -d" " -f6)
        fi  
        echo  "export to Netezza $tname succeeded."
        echo -e "App Name: $APP_NAME\nJob Name: $SQ_JOB_NAME\nBatchName: $BATCH_NAME\nLog File: $LOG_FILE\nStatus: $JOB_STATUS\nTime Taken: $time_taken(sec)\nDate: $date\nTable Name:$tname\nRecords Count:$count\n"  >> $EMAIL_REPORT
        report_job_run "Success"
    else
	echo  "export to Netezza $tname failed."
        message=""
        j=0
        while [ $RETRY -gt j ]
        do
            for index in "${!error_msgs[@]}"; do
	        error=${error_msgs["$index"]}
	        if [ $(cat $LOG_FILE | grep -i "$error" | wc -w) -gt 0 ]; then
	 	    echo Log file contains error $error
		    LOG_FILE=$LOG_FILE-RESTART-$j
		    $SQOOP_PATH/sqoop export $PARAMLIST >> $LOG_FILE 2>&1
		    RC=$?
		    break
	        else
	            echo Log file does not contain error : $error
	        fi
	    done

	    let j=j+1
        done
    fi

fi

if [ $RC -ne 0 ]; then
    JOB_STATUS="Failed"
            
   if [ $(cat $LOG_FILE | grep -i "FAILED" | wc -w) -gt 0 ]; then
   	message=$(cat $LOG_FILE | grep -i "FAILED")
       	#message=$(echo $message | cut -d":" -f2)
   	message=$(echo $message | sed -s "s/'//g")
   fi
   
   if [ $(cat $LOG_FILE | grep -i "ERROR" | wc -w) -gt 0 ]; then
   	message=$(cat $LOG_FILE | grep -i "ERROR")
   	message=$(echo $message | sed -s "s/'//g")
   	echo "message: " $message
   fi
   
   if [ $(cat $LOG_FILE | grep -i "Submitted application" | wc -w) -gt 0 ]; then
        application=$(cat $LOG_FILE | grep -i "Submitted application")
        echo "application : " $application
        app_id=$(echo $application | cut -d" " -f7)
        YARN_FILE_NAME="$ZEKE_JOB_NAME-$ID-$app_id.log"
        if [ "$JOB_PATH" == "-" ]
        then
            YARN_LOG_FILE=""$LOG_PATH"/"$YARN_FILE_NAME""
        else
            YARN_LOG_FILE=""$LOG_PATH"/"${JOB_PATH}"/"$YARN_FILE_NAME""
        fi
   	echo "app_id :  " $app_id
   	echo "sleeping for 180 sec"
        sleep 180 
        message=`yarn logs -applicationId $app_id | sed -n '/bad records/{n;p;n;p;n;p;n;p;}'`
        message=${message:0:500}
   	yarn logs -applicationId $app_id >> $YARN_LOG_FILE
   	RC=$?
        echo "Yarn RC : " $RC
        if [ $RC -ne 0 ]; then
   	     echo "Yarn retry:"
   	     sleep 180
   	     yarn logs -applicationId $app_id >> $YARN_LOG_FILE
   	fi
   	hadoop fs -put $YARN_LOG_FILE $YARN_LOGS_LOC
   fi
   
   if [[ -z "$app_id" ]]; then
        echo -e "App Name: $APP_NAME\nJob Name: $SQ_JOB_NAME\nBatch Name: $BATCH_NAME\nLog File: $LOG_FILE\nStatus: $JOB_STATUS\nMessage: $message\nTime Taken: $time_taken(sec)\nDate: $date\n" >> $EMAIL_REPORT
   else
        echo -e "App Name: $APP_NAME\nJob Name: $SQ_JOB_NAME\nBatch Name: $BATCH_NAME\nLog File: $LOG_FILE\nStatus: $JOB_STATUS\nMessage: $message\nTime Taken: $time_taken(sec)\nDate: $date\nYarn Log File: $YARN_FILE_NAME\nHDFS Loc: $YARN_LOGS_LOC\nAPP ID:$app_id" >> $EMAIL_REPORT
   fi
	
   report_job_run "Failed "

fi

END_TIME=$(date +"%Y-%m-%d %T")

##########   Load stats into Detail table ############
SQLSTMT="insert into $DETAIL_TABLE(job_id,Table_Name,Num_Rows_Loaded,Total_Size,Time_Take,cycle_id) values ($JOB_ID,'$tname',$count, NULL, '$time_taken','$CYCLE_ID');"

echo "SQLSTMT : " $SQLSTMT
execute_sql "$SQLSTMT" SQLRESULT RC
RC=$?
echo "DETAIL TABLE RC :" $RC
message=${message:0:5000}
##########   Load stats into Summary table ############
SQLSTMT="insert into $SUMMARY_TABLE (job_id,job_name,target,user_id,server_name,job_start_time,job_end_time,message,status,cycle_id) values ($JOB_ID,'$SQ_JOB_NAME','$TYPE','$user_id','$server_name','$START_TIME','$END_TIME','$message', '$JOB_STATUS','$CYCLE_ID');"

echo "SQLSTMT : " $SQLSTMT
execute_sql "$SQLSTMT" SQLRESULT RC
RC=$?
echo "SUMMARY TABLE RC :" $RC


if [[  -e $DETAIL_REPORT ]]; then
   rm $DETAIL_REPORT
fi

if [[  -e $PARAM_FILE ]]; then
   rm $PARAM_FILE
fi

if [[  -e $PARAM_LOG_FILE ]]; then
   rm $PARAM_LOG_FILE
fi

if [[  -e $EMAIL_REPORT ]]; then
 rm $EMAIL_REPORT
fi

if [[  -e $SUMMARY_REPORT ]]; then
 rm $SUMMARY_REPORT
fi

if [[  -e $YARN_LOG_FILE ]]; then
 rm $YARN_LOG_FILE
fi

if [[ $ZEKE_RC -ne 0 ]]; then
   echo " ZEKE Exit RC : " $ZEKE_RC
   exit 1
fi




###### wrapper dq ######
#!/bin/ksh
#########################################################
# Wrapper Script for DQ Jobs
# 
# Zeke will invoke this script by passing Zeke Job Name
# as a parameter.
# 
# 
# 
# 
#
# Revision History
# 0.1 02/07/2016 Initial draft.
# 0.2 03/07/2016 Modified delimiter table, partition parameter parsing
#
#########################################################
#set -x
ZEKE_JOB_NAME=$1
ROOT_PATH='/u01/datascience/common/bin'
#ROOT_PATH='/users/s061332'
. $ROOT_PATH/functions/commonutil.ksh

TMP='/tmp'
ID=$RANDOM

user_id=$(whoami)
server_name=$(hostname)

ENV_FILE="$ROOT_PATH/common.env"
DQ_ENV_FILE="$ROOT_PATH/dq.env"

echo "ZEKE_JOB_NAME :" $ZEKE_JOB_NAME
echo "ENV_FILE : " $ENV_FILE 

if [[ -z $ZEKE_JOB_NAME ]]; then
  echo "Error: Missing zeke-job-nam parameter."
  echo "USAGE: wrapper_ds.ksh zeke-job-name "
  exit 1
fi

source $ENV_FILE
source $DQ_ENV_FILE
if [ $? -ne 0 ]; then
  echo "Error: Missing env file."
  exit 1
fi

sid=$user_id
echo "sid:" $sid
usid=$(echo $sid | tr '[:lower:]' '[:upper:]')
echo "usid : " $usid

/usr/bin/kinit $usid@AETH.AETNA.COM -k -t /users/$sid/$sid.keytab

date=`date -d"-1 days" +%Y-%m-%d`
yr=`echo $date | awk -F\- '{print $1}'`
mth=`echo $date | awk -F\- '{print $2}'`
dy=`echo $date | awk -F\- '{print $3}'`

LOG_LEVEL=INFO

PROGRESS="InProgress"
COMPLETE="Complete"

START_TIME=$(date  +%Y-%m-%d_%T)
START_TIME_S=$SECONDS

PARAM_LOG_FILE=""$TMP"/"$ZEKE_JOB_NAME-$START_TIME-$ID-PARAMS".log"
echo "Log file : " $PARAM_LOG_FILE

PARAM_FILE=""$TMP"/"$ZEKE_JOB_NAME-$START_TIME-$ID-PARAM".txt"
echo "Param file : " $PARAM_FILE

ERRORLIST_FILE=""$TMP"/"$ZEKE_JOB_NAME-$START_TIME-$ID-ERRORLIST".txt"
EMAIL_REPORT=""$TMP"/"$ZEKE_JOB_NAME-$START_TIME"-email-report-$ID.txt"
echo "Email report file: " $EMAIL_REPORT

SV="schemavalidator"
NC="nullcounter"
DI="duplicateidentifier"

typeset -A error_msgs

##########  Get the job attributes from the Job Master table ############

SQLSTMT="select j.job_name, a.email , a.script_path , a.log_path , j.tools_used , j.retry_attempt ,coalesce(nullif(j.op_stats_table,''),'NULL') , a.app_name , j.job_id , a.yarn_logs ,a.app_id, j.batchid, j.batchname  from $APP_TABLE as a join $JOB_TABLE as j on j.app_id=a.app_id  where j.Zeke_Job_Name='$ZEKE_JOB_NAME';"

echo "SQL statement: " $SQLSTMT
execute_sql "$SQLSTMT" SQLRESULT RC
  echo "RC: " $RC
  if [ $RC -ne 0 ]; then
    echo "inside" 
    EMAIL_REPORT=""$TMP"/temp.txt"
    EMAIL_TO=$EMAIL_ADDR

    echo "Job $ZEKE_JOB_NAME failed to load parameters."
    echo -e "Message: Failed to load parameters from app & job tables." >> $EMAIL_REPORT
    report_job_run "Failed "

    echo "EMAIL_REPORT :" $EMAIL_REPORT
    echo "EMIL_TO : " $EMAIL_TO

    if [[  -e $EMAIL_REPORT ]]; then
     rm $EMAIL_REPORT
    fi

    exit 1 
  fi
echo $SQLRESULT | read DQ_JOB_NAME EMAIL_TO SCRIPT_PATH LOG_PATH TYPE RETRY STATS_TABLE APP_NAME JOB_ID YARN_LOGS_LOC APP_ID BATCH_ID BATCH_NAME
echo JOB_NAME is $DQ_JOB_NAME
echo EMAIL_TO is $EMAIL_TO
echo SCRIPT_PATH is $SCRIPT_PATH
echo LOG_PATH is $LOG_PATH
echo TYPE is $TYPE
echo RETRY is  $RETRY
echo STATS_TABLE is $STATS_TABLE
echo APP_NAME is $APP_NAME
echo JOB_ID is $JOB_ID
echo YARN_LOGS_LOC is $YARN_LOGS_LOC
echo APP_ID is $APP_ID
echo BATCH_ID is $BATCH_ID
echo BATCH_NAME is $BATCH_NAME

######## Set the log directory path and the log file name. ##########
LOG_FILE=""$LOG_PATH"/"$DQ_JOB_NAME-$START_TIME-$ID".log"
echo "Log file : " $LOG_FILE

DETAIL_REPORT=""$TMP"/"$DQ_JOB_NAME"-detail-report-"$ID".txt"

job_start_time=$(date +%Y-%m-%d_%T)


##########  Get the cycle  attributes from the Job Cycle table ############
SQLSTMT="select status,cycle_id,app_id,batchid from $CYCLE_TABLE where app_id='$APP_ID'and batchid='$BATCH_ID' and status='InProgress' order by cycle_id desc limit 1 ;"

echo SQLSTMT is $SQLSTMT
execute_sql "$SQLSTMT" SQLRESULT RC
  echo "RC: " $RC
  if [ $RC -ne 0 ]; then
    echo "Eror retrieving status: "
  fi 

echo $SQLRESULT | read STATUS CYCLE_ID APP_ID B_ID
echo STATUS is $STATUS
echo CYCLE_ID is $CYCLE_ID
echo APP_ID is $APP_ID
echo CYCLE BATCH_ID is $B_ID

if [[ "$STATUS" != $PROGRESS ]]; then
	echo Error: Cycle is not in progress, Please open the cycle and rerun !
        exit 1
fi

#########  Get  error list from the error  Master table ############
SQLSTMT="select script_type, error from $ERROR_TABLE where script_type='dq'";
echo "SQL statement: " $SQLSTMT
SQLRESULT=`mysql --skip-column-names -h$HOST --port $PORT  -u$USER -p$PW -e "$SQLSTMT"`
RC=$?
echo "RC: " $RC
if [ $RC -ne 0 ]; then
    echo "inside"
    EMAIL_REPORT=""$TMP"/temp.txt"
    EMAIL_TO=$EMAIL_ADDR

    echo "Job $ZEKE_JOB_NAME failed to load error list."
    echo -e "Message: Failed to load parameters from error table." >> $EMAIL_REPORT
    report_job_run "Failed "

    echo "EMAIL_REPORT :" $EMAIL_REPORT
    echo "EMIL_TO : " $EMAIL_TO

    if [[  -e $EMAIL_REPORT ]]; then
     rm $EMAIL_REPORT
    fi

    exit 1
fi
echo "$SQLRESULT" >> $ERRORLIST_FILE
k=0
######### Construct error list #########
cat $ERRORLIST_FILE | while read line
 do
  k=`expr $k + 1`
  script_type=`echo $line | cut -f1 -d' '`
  error=`echo $line | cut -f2- -d' '`
  if [[ "$script_type" == 'dq' ]]; then
        echo script type is dq....
        error_msgs[$k]=$error
  fi
done


#########  Get the param  attributes from the Job Param table ############
SQLSTMT="select p.param_name,p.param_value from $PARAM_TABLE as p join $JOB_TABLE as j on p.job_id=j.job_id where j.Zeke_Job_Name='$ZEKE_JOB_NAME';"
echo "SQL statement: " $SQLSTMT
SQLRESULT=`mysql --skip-column-names -h$HOST --port $PORT  -u$USER -p$PW -e "$SQLSTMT"`
RC=$?
echo "RC: " $RC
if [ $RC -ne 0 ]; then
    echo "inside" 
    EMAIL_REPORT=""$TMP"/temp.txt"
    EMAIL_TO=$EMAIL_ADDR

    echo "Job $ZEKE_JOB_NAME failed to load parameters."
    echo -e "Message: Failed to load parameters from param table." >> $EMAIL_REPORT
    report_job_run "Failed "

    echo "EMAIL_REPORT :" $EMAIL_REPORT
    echo "EMIL_TO : " $EMAIL_TO

    if [[  -e $EMAIL_REPORT ]]; then
     rm $EMAIL_REPORT
    fi

    exit 1 
  fi

echo "$SQLRESULT" >> $PARAM_FILE

typeset -A params
typeset -A params_hash

######### Construct parameter list #########
cat $PARAM_FILE | while read line
 do
  echo "line:" $line
  F1=`echo $line | cut -f1 -d' '`
  F2=`echo $line | cut -f2- -d' '`
  echo F1 is $F1
  if [[ "$F1" == 'type' ]]; then
     TYPE2=$F2
  fi
done

echo TYPE is $TYPE2

if [[ -z "$TYPE2" ]]; then
    echo -e "Error: Missing Type parameter !" >> $EMAIL_REPORT
    report_job_run "Failed"
    exit 1

fi

echo "Log file : " $LOG_FILE

cat $PARAM_FILE | while read line
 do
  #echo "line:" $line
  F1=`echo $line | cut -f1 -d' '`
  F2=`echo $line | cut -f2- -d' '`
  #echo F1 is $F1
  if [[ "$F1" == 'tables' ]]; then
     IFS=","
     echo tables is ...
     set -A tabs $F2
     echo tab1 ${tabs[0]}
     len1=${#tabs[@]}
  fi
  if [[ "$F1" == 'partitions' ]]; then
     IFS=","
     echo partitions...
     set -A parts $F2
     echo tab1 ${parts[0]}
     len2=${#parts[@]}
  fi
  unset IFS
  if [[ "$F1" == 'app_id' ]]; then
     APP_ID2=$F2
  fi

  if [[ "$F1" == 'env' ]]; then
     ENV2=$F2
  fi

  if [[ "$F1" == 'queue' ]]; then
     QUE=$F2
  fi

  if [[ "$F1" == 'log_level' ]]; then
     LOG_LEVEL=$F2
  fi

done

echo APP_ID2 is $APP_ID2
echo ENV2 is $ENV2
echo QUE is $QUE
echo TYPE2 is $TYPE2
echo LOG_LEVEL is $LOG_LEVEL


if [[ "$len1" != "$len2" ]]; then
    echo -e "Error: tables, partitions count mismatch !" >> $EMAIL_REPORT
    report_job_run "Failed"
    exit 1

fi
if [[ -z "$APP_ID2" ]]; then
    echo -e "Error: Missing app_id in params table !" >> $EMAIL_REPORT
    report_job_run "Failed"
    exit 1

fi 
 
if [[ -z "$ENV2" ]]; then
    echo -e "Error: Missing env parameter !" >> $EMAIL_REPORT
    report_job_run "Failed"
    exit 1

fi 

if [[ -z "$QUE" ]]; then
    echo -e "Error: Missing queue parameter !" >> $EMAIL_REPORT
    report_job_run "Failed"
    exit 1

fi 


c=0
	while [[ $c -le $len1-1 ]]; do
	  echo Table ${tabs[$c]}
	  PARAMLIST=$(echo $PARAMLIST${tabs[$c]}${parts[$c]}'')
	  (( c++ ))
	done


PARAMLIST=$(echo ${PARAMLIST} $APP_ID2 $ENV2 $QUE $LOG_LEVEL) 
echo PARAMLIST is $PARAMLIST

echo "Job_Name: " $DQ_JOB_NAME
echo "Param List :" $PARAMLIST


if [[ "$TYPE2" == "$SV" ]]; then
	echo Schemavalidate in progress !
        #sh schemavalidator.sh $PARAMLIST  >> $LOG_FILE 2>&1
        hadoop jar ${ROOT_PATH}/jar/dqcheck-2.0.jar com.aetna.dqcheck.driver.StructuralProfileDriver -libjars $LIBJARS $PARAMLIST  >> $LOG_FILE 2>&1

	RC=$?
	echo RC: $RC
fi

if [[ "$TYPE2" == "$NC" ]]; then
       echo nullcounter job in progress !
        echo Null counter param list is:  $PARAMLIST
        #sh ./nullcounter.sh $PARAMLIST >> $LOG_FILE 2>&1
	hadoop jar ${ROOT_PATH}/jar/dqcheck-2.0.jar com.aetna.dqcheck.driver.NullProfileDriver -libjars $LIBJARS $PARAMLIST >> $LOG_FILE 2>&1
        RC=$?
	echo RC: $RC
fi

if [[ "$TYPE2" == "$DI" ]]; then
       echo duplicateidentifier job in progress !
        echo Duplicate identifier param list is:  $PARAMLIST

        #sh ./dupsidentifier.sh $PARAMLIST >> $LOG_FILE 2>&1 
	hadoop jar ${ROOT_PATH}/jar/dqcheck-2.0.jar com.aetna.dqcheck.driver.DuplicateDriver -libjars $LIBJARS  $PARAMLIST >> $LOG_FILE 2>&1
        RC=$?
	echo RC: $RC
fi

j=0
while [ $RETRY -gt j ]
 do
	for index in "${!error_msgs[@]}"; do
		error=${error_msgs["$index"]}

		if [ $(cat $LOG_FILE | grep -i "$error" | wc -w) -gt 0 ]; then
			 echo Log file contains error $error
			 echo "Retry # : " $j
			 LOG_FILE=$LOG_FILE-RESTART-$j

			 if [[ "$TYPE2" == "$SV" ]]; then
				#sh ./schemavalidator.sh $PARAMLIST  >> $LOG_FILE 2>&1
        			hadoop jar ${ROOT_PATH}/jar/dqcheck-2.0.jar com.aetna.dqcheck.driver.StructuralProfileDriver -libjars $LIBJARS $PARAMLIST  >> $LOG_FILE 2>&1
  			 fi
			 if [[ "$TYPE2" == '$NC' ]]; then
				#sh ./nullcounter.sh $PARAMLIST >> $LOG_FILE 2>&1
				hadoop jar ${ROOT_PATH}/jar/dqcheck-2.0.jar com.aetna.dqcheck.driver.ProfileDriver -libjars $LIBJARS $PARAMLIST >> $LOG_FILE 2>&1
			        echo Type is: $NC                                
                         fi
			 if [[ "$TYPE2" == '$DI' ]]; then
				 #sh ./dupsidentifier.sh $PARAMLIST >> $LOG_FILE 2>&1
                        	 hadoop jar ${ROOT_PATH}/jar/dqcheck-2.0.jar com.aetna.dqcheck.driver.DuplicateDriver -libjars $LIBJARS  $PARAMLIST >> $LOG_FILE 2>&1

			 fi
			 break
		else
			echo Log file does not contain error : $error
		fi
	done

	let j=j+1
done
echo RC is  $RC
ZEKE_RC=$RC

END_TIME_S=$SECONDS
time_taken=$((END_TIME_S- START_TIME_S))
message=""

#RC=2
#LOG_FILE="/u01/datascience/Sample/logs/sample_dq.log"
echo LOG_FILE is $LOG_FILE

if [ $RC -ne 0 ]; then
	echo "Job Failed.."
	JOB_STATUS="Failed"
                
	if [ $(cat $LOG_FILE | grep -i "FAILED" | wc -w) -gt 0 ]; then
		message=$(cat $LOG_FILE | grep -i "FAILED")
		message=$(echo $message | sed -s "s/'//g")
	fi

	if [ $(cat $LOG_FILE | grep -i "Exception" | wc -w) -gt 0 ]; then
		message=$(cat $LOG_FILE | grep -i "Exception")
		message=$(echo $message | sed -s "s/'//g")
		echo "message: " $message
	fi

	if [ $(cat $LOG_FILE | grep -i "Submitted application" | wc -w) -gt 0 ]; then
	   application=$(cat $LOG_FILE | grep -i "Submitted application")
	   echo "application : " $application
	   app_id=$(echo $application | cut -d" " -f7)
	   YARN_FILE_NAME="$ZEKE_JOB_NAME-$ID-$app_id.log"
	   YARN_LOG_FILE=""$LOG_PATH"/"$YARN_FILE_NAME""
	   echo "app_id :  " $app_id
	   echo "sleeping for 180 sec"
	   sleep 180 
	   yarn logs -applicationId $app_id >> $YARN_LOG_FILE
	   RC=$?
	   echo "Yarn RC : " $RC
	   if [ $RC -ne 0 ]; then
		 echo "Yarn retry:"
		 sleep 180
		 yarn logs -applicationId $app_id >> $YARN_LOG_FILE
	   fi
	   hadoop fs -put $YARN_LOG_FILE $YARN_LOGS_LOC
	fi
	if [[ -z "$app_id" ]]; then
			  echo -e "App Name: $APP_NAME\nJob Name: $DQ_JOB_NAME\nBatch Name: $BATCH_NAME\nLog File: $LOG_FILE\nStatus: $JOB_STATUS\nMessage: $message\nTime Taken: $time_taken(sec)\nDate: $date\n" >> $EMAIL_REPORT
	else
		   echo -e "App Name: $APP_NAME\nJob Name: $DQ_JOB_NAME\nBatch Name: $BATCH_NAME\nLog File: $LOG_FILE\nStatus: $JOB_STATUS\nMessage: $message\nTime Taken: $time_taken(sec)\nDate: $date\nYarn Log File: $YARN_FILE_NAME\nHDFS Loc: $YARN_LOGS_LOC\nAPP ID:$app_id" >> $EMAIL_REPORT
	fi
		report_job_run "Failed "
else
	JOB_STATUS="Success"
	echo  "Job completed successfully.."
	echo -e "App Name: $APP_NAME\nJob Name: $DQ_JOB_NAME\nBatchName: $BATCH_NAME\nLog File: $LOG_FILE\nStatus: $JOB_STATUS\nTime Taken: $time_taken(sec)\nDate: $date\n"  >> $EMAIL_REPORT
		report_job_run "Success"
fi

END_TIME=$(date +"%Y-%m-%d %T")

##########   Load stats into Detail table ############
SQLSTMT="insert into $DETAIL_TABLE(job_id,Total_Size,Time_Take,cycle_id) values ($JOB_ID, NULL, '$time_taken','$CYCLE_ID');"

echo "SQLSTMT : " $SQLSTMT
execute_sql "$SQLSTMT" SQLRESULT RC
RC=$?
echo "DETAIL TABLE RC :" $RC
message=${message:0:5000}
##########   Load stats into Summary table ############
SQLSTMT="insert into $SUMMARY_TABLE (job_id,job_name,target,user_id,server_name,job_start_time,job_end_time,message,status,cycle_id) values ($JOB_ID,'$DQ_JOB_NAME','$TYPE','$user_id','$server_name','$START_TIME','$END_TIME','$message', '$JOB_STATUS','$CYCLE_ID');"

echo "SQLSTMT : " $SQLSTMT
execute_sql "$SQLSTMT" SQLRESULT RC
RC=$?
echo "SUMMARY TABLE RC :" $RC


if [[  -e $ERRORLIST_FILE ]]; then
 rm $ERRORLIST_FILE
fi

if [[  -e $DETAIL_REPORT ]]; then
   rm $DETAIL_REPORT
fi

if [[  -e $PARAM_FILE ]]; then
   rm $PARAM_FILE
fi

if [[  -e $PARAM_LOG_FILE ]]; then
   rm $PARAM_LOG_FILE
fi

if [[  -e $YARN_LOG_FILE ]]; then
   rm $YARN_LOG_FILE 
fi

if [[  -e $EMAIL_REPORT ]]; then
 rm $EMAIL_REPORT
fi

if [ $ZEKE_RC -ne 0 ]; then
   echo " ZEKE Exit RC : " $ZEKE_RC
         exit 1
fi
                
