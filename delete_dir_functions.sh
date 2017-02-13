rm -f delete_dir_temp1
rm -f delete_dir_temp2

echo "Please Enter the Absolute Path of Directory which you want to delete : "
read directory
echo "Enter the expected Number of Files inside $directory : "
read Number


input_validation(){
	#validate the directory
	hdfs dfs -ls -R $1 >> delete_dir_temp2
		if [ "$?" -ne 0 ];
			then
			echo "--------------------Directory not found-------------------"
			exit 1
		fi
	
	#validate the number
	[[ "$2" != ?(+|-)+([0-9]) ]] && echo "$2 is not numeric, Enter a valid number" && exit 1
	return 10
}

input_validation $directory $Number
echo "$?"
########################################################################################
#------------------------------
#printing the details
#------------------------------
cat delete_dir_temp2 |cut -c 1 >> delete_dir_temp1
var_dir=$(cat delete_dir_temp1 |grep d |wc -l)
var_file=$(cat delete_dir_temp1 |grep - |wc -l)
var_total=$(cat delete_dir_temp1|wc -l)
echo "----------------------------------------------------------"
echo "There are total : $var_total - items in $directory "
echo "  out of which  : $var_dir - directories "
echo "                : $var_file -  files"
echo "----------------------------------------------------------"
########################################################################################

pre_delete_check(){
	#Direcories inside directory -------------------------------------------------------
	if [ $1 -ne 0 ];
		then
		echo ""
		echo "There are $1 directories inside $directory";
		echo "---------------Can not delete the directory---------------"
		exit 1
	fi
	#Mismatch between expected number of files with actual number of files--------------
	if [ $2 -lt $3 ];
		then
        echo "There are more files then your expected number of files"
		exit 1
	elif [ $2 -gt $3 ];
		then
        echo "There are less files then your expected number of files"
		exit 1
	fi
	return 11
}
pre_delete_check $var_dir $Number $var_file
echo "$?"

#deleting the files--------------------------------------------------------------------
delete_dir(){
echo "Following $1 Files will be deleted"
echo "----------------------------------------------------------"
cat delete_dir_temp2 |awk -F"/" '{print $NF}'
echo "----------------------------------------------------------"
echo ""
echo "Are you sure you want to delete this files ? (y/n) : "
read choice
if [ "$choice" = "y" ];
    then
    echo "yyyyyyyy"
    echo "Deleting the Files"
else
    echo "---------------Can not delete the directory---------------";
fi
}
delete_dir $Number

