#!/bin/bash

###############################################################################
##Script Name : high_volume_tables.sh
##Description : Find out the high volume tables /files  present in the HDFS and their
##		details like owner,create time,location and the size.
##Create Date : 
##Modified date : 
##version : 1.0
###############################################################################

rm -f tablenames_hive_temp
rm -f tablenames_hive
rm -f script_hive
rm -f script_hiveop
rm -f hive_details
rm -f tablenames_hdfs
rm -f hdfs_details
rm -f diff_hive_hdfs
rm -f output_hdfs.csv
rm -f output_hive.csv
rm -f temp1


echo "Please Enter the Database Name : "
read database
echo "Enter the Number of Tables : "
read top
echo "Enter the Mail_id where the output will be send"
read mail_id
echo "      "
echo "selected database : $database"
echo "no of tables      : $top"
echo "Mail_id : $mail_id"
echo " "

#checking the input (no of tables)
[[ "$top" != ?(+|-)+([0-9]) ]] && echo "$top is not numeric, Enter a valid number" && exit 1

############# variables #############
var_use="use "
var_semicolon=";"
var_show_tables="show tables"
var_describe_formatted="describe formatted "
var_dot="."
var_tablenames="/tablenames_hive"
var_current_directory=$(pwd)
######################################
echo "current directory :$var_current_directory "
echo "      "

#-----------------------------------------------------------------------------------
# Hive script and execute :This will show the tables present in the database
# Use database;show tables
#-----------------------------------------------------------------------------------

echo $var_use$database$var_semicolon >> tablenames_hive_temp
echo $var_show_tables$var_semicolon >> tablenames_hive_temp
echo "Hive script is running for fetching tablenames"
echo " "
hive -f tablenames_hive_temp >>tablenames_hive
if [ "$?" -eq 0 ];
	then
	echo " "
	echo "-------- Tablenames succesfully fetched from $database --------"
	echo " "
else
	echo " "
	echo "-------- $database : Database not found --------"
	echo " "
	exit 1;
fi

#-----------------------------------------------------------------------------------
# Hive script and execute : This will print the describe formatted details from Hive
# of all the tables in script_hiveop
#-----------------------------------------------------------------------------------

echo "hive script is running for fetching the Owner ,createtime and the location"
echo " "
while read line
	do
	echo $var_describe_formatted$database$var_dot$line$var_semicolon>>script_hive
	done <$var_current_directory$var_tablenames
hive -f script_hive >>script_hiveop

#-----------------------------------------------------------------------------------
#var_hdfs_location : extract the hdfs location of the database from script_hiveop
#output1 : extract the Owner,CreateTime,Location of tables from script_hiveop
#-----------------------------------------------------------------------------------

var_hdfs_location=$(cat script_hiveop| grep "Location" |head -1|awk '$1=$1'|sed -e 's/Location: //g'|sed 's/[^/]*$//')
echo " "
echo "hdfs location for the $database is $var_hdfs_location"
echo " "
grep "Owner\|CreateTime\|Location" script_hiveop |awk '$1=$1'|sed -e 's/Owner: /#/;s/CreateTime: //g;s/Location: //g'|tr "\n" ",,,"|tr "#" "\n"|tr "," " "|sed -e ':a' -e 'N' -e '$!ba' -e 's/\n//1' >> hive_details

#-----------------------------------------------------------------------------------
#tablenames_hdfs : extract the table and directory names from hdfs location
#output2 : size and location of all tables from the hdfs location
#-----------------------------------------------------------------------------------

hdfs dfs -du $var_hdfs_location |tr -s " "|sort -n -r|head -$top |cut -d " " -f1,2>> hdfs_details
cat hdfs_details |awk -F"/" '{print $NF}'>>tablenames_hdfs

#-----------------------------------------------------------------------------------
# diff_hive_hdfs : directories present in the database location which are not tables
#-----------------------------------------------------------------------------------

awk 'NR==FNR{a[$0];next} ! ($0 in a)' tablenames_hive tablenames_hdfs >>diff_hive_hdfs
paste tablenames_hdfs hdfs_details >> temp1
awk 'FNR==NR{A[$1]=$0;next} ($1 in A){print A[$1]}' temp1 diff_hive_hdfs||sed -e 's/\t/,/g;s/ /,/g'>>output_hdfs.csv

#-----------------------------------------------------------------------------------
#joining the hive_details(owner,location,createtime) with hdfs_details(size,location)
#-----------------------------------------------------------------------------------

awk 'FNR==NR{A[$2]=$0;next} ($8 in A){print A[$8] FS $1,$2,$3,$4,$5,$6,$7}' hdfs_details hive_details|sort -n -r |sed -e 's/ /#/g4'|sed -e 's/ /,/g'|sed -e 's/#/ /g' >>output_hive.csv

var_no_of_directories=$(cat diff_hive_hdfs|wc -l)
var_no_of_tables=$(cat output |wc -l)
echo " "
echo "Top $var_no_of_tables tables in term of size are at $var_current_directory/output_hive"
echo "Top $var_no_of_directories directories in terms of size are at $var_current_directory/output_hdfs"
echo " "
#-----------------------------------------------------------------------------------
#Sending the Outputs to the mail_id
#-----------------------------------------------------------------------------------
echo "Please find the attachments for the high volume tables in output_hive and high volume directories (if any) in output_hdfs for the database $database" |mail -s "HDFS space consumption" -a '$var_current_directory/output_hive.csv' -a '$var_current_directory/output_hdfs.csv' $mail_id
#-----------------------------------------------------------------------------------
#removing the temp Files
#-----------------------------------------------------------------------------------
rm -f tablenames_hive_temp
rm -f tablenames_hive
rm -f script_hive
rm -f script_hiveop
rm -f hive_details
rm -f tablenames_hdfs
rm -f hdfs_details
rm -f temp1
