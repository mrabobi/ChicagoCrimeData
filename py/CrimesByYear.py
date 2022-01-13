from CrimeStatisticsMain import *
if len(sys.argv)<4:
    print("Invalid arguments number, please specify the dataset path,the property to group the entries by and the file to save the output to")
else:
    spark,data=load_dataframe(sys.argv[1],"Chicago Crime Statistics "+sys.argv[2])
    result=get_crimes_by_property(data,spark,sys.argv[2])
    save_To_File(result,sys.argv[3])
    if len(sys.argv) == 5:
        print(result)
        save_Df_To_Csv(result,sys.argv[4])

# spark-submit CrimesByYear.py Crimes.csv Year CrimesByYear.txt CrimesByYear.csv
# spark-submit CrimesByYear.py Crimes.csv PrimaryType PrimaryType.txt
