###Input 1: TableName ###
echo "Enter the table name to be analysed"
read TableName

###Input 2: Colums_required ###
Colums_required='NA'
exit_flag=0
while [ $exit_flag == 0 ]
do
echo "Do you want to specify the column names(Y/N) "
read Colums_required
if [[ $Colums_required = 'Y' || $Colums_required = 'N' ]]; then
exit_flag=1
else
echo "Input can be either Y or N."
fi
done 

###Input 3: ColumnNames ###
if [[$Colums_required == 'Y']]; then
echo "Enter the column names for which you want the analysis to be done "
read ColumnNames
fi

###Input 4: Frequency of table run ###
echo "How frequently is this table run?"
echo "If not known enter NA"
read Frequency
if [[ $Frequency == 'NA' ]]; then 
echo "frequency not know"
else
$Frequency='NA'
fi

if [[ $Colums_required == 'Y' ]]; then
pyspark python_4.py -i $TableName $Colums_required $ColumnNames $Frequency
else
pyspark python_4.py -i $TableName $Colums_required $Frequency
fi
