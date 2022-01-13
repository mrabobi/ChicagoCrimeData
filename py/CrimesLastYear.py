from datetime import date
from CrimeStatisticsMain import *

todays_date = date.today()
last_year=todays_date.year-5

if len(sys.argv)<3:
    print("Invalid arguments number, please specify the dataset path,the property to group the entries by and the file to save the output to")
else:
    spark,data=load_dataframe(sys.argv[1],"Chicago Crime Statistics from"+sys.argv[2])
    result=get_by_value_e_df(data,"Year", last_year)
    #print(result)
    save_To_File(result,sys.argv[2])
    if len(sys.argv) == 4:
    	save_Df_To_Csv(result, sys.argv[3])
