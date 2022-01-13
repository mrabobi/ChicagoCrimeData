from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col,to_date,to_timestamp,when,hour,month,year,max,array
import sys
import pandas as pd
from pyspark import SparkConf, SparkContext

def load_dataframe(path,appName):

    spark = SparkSession.builder.appName(appName).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    data = spark.read.option("header", "True").option("inferSchema","True").csv(path).na.drop()
    data = data.withColumn('DateTime', to_timestamp('date', 'MM/dd/yyyy hh:mm:ss a'))
    data=data.drop("Date")
    data=data.withColumnRenamed("DateTime","Date")
    data=get_month_from_datetime(data,spark,"Date","Month")
    data=get_hour_from_datetime(data,spark,"Date","Hour")
    return spark,data

def prepare_data_for_feature_vector(data,spark,includeArrest=False):
    data2=convert_column_index(data,spark,"PrimaryType")
    data2=convert_column_index(data2,spark,"District")
    data2=convert_column_index(data2,spark,"LocationDescription")
    data2=convert_column_index(data2,spark,"Description")
    data2=data2.withColumn("DomesticInt", when((data.Domestic == True), 1).otherwise(0))
    data2=data2.drop("Domestic")
    data2=data2.withColumnRenamed("DomesticInt","Domestic")
    if includeArrest:
        data2=data2.withColumn("ArrestInt", when((data.Arrest == True), 1).otherwise(0))
        data2=data2.drop("Arrest")
        data2=data2.withColumnRenamed("ArrestInt","Arrest")
    return data2

def convert_arrest_to_label_column(data):
    data2=data.withColumn("label", when((data.Arrest == True), 1).otherwise(0))
    return data2


def get_by_contains_clause(df,column,keyword):
    return df.filter(col(column).contains(keyword.upper())).collect()

def get_by_property_true(df,column):
    return df.filter(col(column)).collect()

def get_by_property_false(df,column):
    return df.filter(col(column)==False).collect()

def get_by_value_ge(df,column,value):
    return df.filter(col(column)>=value).collect()

def get_by_value_le(df,column,value):
    return df.filter(col(column)<=value).collect()

def get_by_value_g(df,column,value):
    return df.filter(col(column)>value).collect()

def get_by_value_l(df,column,value):
    return df.filter(col(column)<value).collect()

def get_by_value_e(df,column,value):
    return df.filter(col(column)==value).collect()

def get_by_value_e_df(df,column,value):
    return df.filter(col(column)==value)

def get_by_value_ne(df,column,value):
    return df.filter(col(column)!=value).collect()

def get_stree_crimes(df):
    return get_by_contains_clause(df,"`Location Description`","STREET")

def get_crimes_by_property(df,spark,property,desc=True,saveToFile=False,folderToSave=""):
    if " " in property:
        property="`"+property+"`"
    order="ASC"
    if desc:
        order="DESC"
    df.createOrReplaceTempView("crimes")
    df2=spark.sql(f"SELECT {property},COUNT(*) AS CrimesCount FROM crimes GROUP BY {property} ORDER BY CrimesCount {order}")
    return df2

def get_crimes_by_year(df,spark):
    df.createOrReplaceTempView("crimes")
    df2=spark.sql("SELECT Year,COUNT(*) AS CrimesCount FROM crimes GROUP BY Year ORDER BY CrimesCount DESC").collect()
    return df2

def get_count_firearm(df,spark):
    df.createOrReplaceTempView("crimes")
    df2=spark.sql("SELECT Year, COUNT(*) FROM crimes WHERE Description LIKE '%FIREARM%' Group by Year").collect()
    return df2

def get_most_frequet_crimes_by_year_and_month(df):
    df.createOrReplaceTempView("crimes")
    aggregrated_table = (
    df.groupby("Year", "Month","PrimaryType")
    .count()
    .withColumn("CrimesNumber", array("count", "PrimaryType"))
    .groupby("Year", "Month")
    .agg(max("CrimesNumber").getItem(1).alias("MostCommonCrime")))
    return aggregrated_table 

def get_most_frequet_crimes_by_year_and_month_and_location(df):
    df.createOrReplaceTempView("crimes")
    aggregrated_table = (
    df.groupby("Year","LocationDescription","PrimaryType")
    .count()
    .withColumn("CrimesNumber", array("count", "PrimaryType"))
    .groupby("Year","LocationDescription")
    .agg(max("CrimesNumber").getItem(1).alias("MostCommonCrime")))
    return aggregrated_table 

def select_columns(df,spark,columns):
    if len(columns)==0:
        return df
    delimiter=","
    columns=delimiter.join(columns)
    df.createOrReplaceTempView("crimes")
    df2=spark.sql("SELECT "+columns+" FROM crimes")
    return df2

def get_unique_values_list(df,spark,column):
    df.createOrReplaceTempView("crimes")
    df2=spark.sql("SELECT DISTINCT "+column+" FROM crimes").collect()
    columns=[]
    for row in df2:
        columns.append(row[0])
    return columns

def convert_column_index(df,spark,inital_column):
    result=get_unique_values_list(df,spark,inital_column)
    indexdf=spark.createDataFrame(zip(result, [i for i in range(len(result))]), schema=[inital_column, inital_column+"Index"])
    indexdf.createOrReplaceTempView("indexTable")
    data2=df.join(indexdf,inital_column).drop(inital_column)
    data2=data2.withColumnRenamed(inital_column+"Index",inital_column)
    return data2

def add_time_from_timestamp(df,spark,timestamp_column,new_column_name,time_format):
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    df=df.withColumn(new_column_name,to_date(col(timestamp_column),time_format))
    return df

def get_hour_from_datetime(df,spark,timestamp_column,new_column_name):
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    df=df.withColumn(new_column_name,hour(col(timestamp_column)))
    return df

def get_year_from_datetime(df,spark,timestamp_column,new_column_name):
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    df=df.withColumn(new_column_name,year(col(timestamp_column)))
    return df

def get_month_from_datetime(df,spark,timestamp_column,new_column_name):
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    df=df.withColumn(new_column_name,month(col(timestamp_column)))
    return df

def get_all_years(df,spark):
    df.createOrReplaceTempView("crimes")
    df2=spark.sql("SELECT Year FROM crimes GROUP BY Year").collect()
    return df2

def save_To_File(df,filePath):
    orig_stdout = sys.stdout
    f = open(filePath, 'w')
    sys.stdout = f
    df.show(truncate=False)
    sys.stdout = orig_stdout
    f.close()

def save_To_File_From_List(df, filePath):
    f = open(filePath, "w")
    for item in df:
        f.write(str(item))
    f.close()

def save_Df_To_Csv(df, filePath):
    df.repartition(1).write.csv(filePath, sep='|');

def lists_to_csv(keyParm1, valueParm1, keyParm2, valueParm2,keyParm3, valueParm3, filePath):
 
    dict = {}  
    if keyParm1 != "-":
        dict[keyParm1] = valueParm1;
    if keyParm2 != "-":
        dict[keyParm2] = valueParm2;
    if keyParm3 != "-":
        dict[keyParm3] = valueParm3;
    df = pd.DataFrame(dict) 
    df.to_csv(filePath) 
