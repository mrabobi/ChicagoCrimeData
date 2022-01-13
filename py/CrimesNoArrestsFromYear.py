from CrimeStatisticsMain import *

# spark-submit CrimesNoArrestsFromYear.py Crimes.csv 2019 NoArrests.txt

if len(sys.argv) < 4:
    print('Invalid arguments number, please specify the dataset path,the property to group the entries by and the file to save the output to')
else:
    (spark, data) = load_dataframe(sys.argv[1],'Chicago Crime Statistics '+ sys.argv[2])
    if sys.argv[2] != 'all':
        result = get_by_value_e_df(data, 'Year', sys.argv[2])
        yesResult = get_by_value_e(result, 'Arrest', 'true')
        noResult = get_by_value_e(result, 'Arrest', 'false')
        answer = ['Number of Arrests from year ' + sys.argv[2],
                  '\n Arrest = TRUE -> ' + str(len(yesResult)),
                  '\n Arrest = FALSE -> ' + str(len(noResult))]
        save_To_File_From_List(answer, sys.argv[3])
    else:
        years = get_all_years(data, spark)
        answer = []
        yearList = []
        yesList = []
        noList = []
        for item in years:
            result = get_by_value_e_df(data, 'Year', item.Year)
            yesResult = get_by_value_e(result, 'Arrest', 'true')
            noResult = get_by_value_e(result, 'Arrest', 'false')
            answer.append('Number of Arrests from year ' + str(item.Year) + '\n Arrest = TRUE -> ' + str(len(yesResult))+'\n Arrest = FALSE -> ' + str(len(noResult)) + '\n')
            yearList.append(item.Year)
            yesList.append(str(len(yesResult)))
            noList.append(str(len(noResult)))
        save_To_File_From_List(answer, sys.argv[3])
        if len(sys.argv) == 5:
            lists_to_csv("year", yearList, "YES", yesList, "NO", noList, sys.argv[4])
            print("test")

        if len(sys.argv) == 6:
            crimeProcent = []
            for i in range(0, len(yesList)):
            	crimeProcent.append( int(yesList[i]) / (int(yesList[i]) + int(noList[i])) * 100 )
            lists_to_csv("year", yearList,"percentage", crimeProcent, "-", "-", sys.argv[5])

