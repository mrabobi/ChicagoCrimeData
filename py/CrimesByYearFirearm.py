from CrimeStatisticsMain import *
if len(sys.argv)<4:
    print("Invalid arguments number, please specify the dataset path,the property to group the entries by and the file to save the output to")
else:
    spark,data=load_dataframe(sys.argv[1],"Chicago Crime Statistics ")
    result=get_count_firearm(data,spark)
    answer = []
    year = []
    value = []
    for item in result:
        answer.append(str(item.Year) + ',' + str(item[1]))
        year.append(str(item.Year))
        value.append(str(item[1]))

    save_To_File_From_List(answer, sys.argv[2])
    if len(sys.argv) == 4:
        lists_to_csv('Year',year,'Count',value,'-','-',sys.argv[3])

# spark-submit CrimesByYearFirearm.py Crimes.csv CrimesByYearFirearm.txt CrimesByYearFirearm.csv

