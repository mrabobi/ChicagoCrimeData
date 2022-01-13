from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col
import sys
import pandas as pd

def load_dataframe(path,appName):
    spark = SparkSession.builder.appName(appName).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    crimes_schema = StructType([StructField("ID", IntegerType(), True),
                                StructField("Case Number", StringType(), False),
                                StructField("Date", DateType(), False ),
                                StructField("Block", StringType(), False),
                                StructField("IUCR", StringType(), False),
                                StructField("Primary Type", StringType(), False  ),
                                StructField("Description", StringType(), False ),
                                StructField("Location Description", StringType(), False ),
                                StructField("Arrest", BooleanType(), False),
                                StructField("Domestic", BooleanType(), False),
                                StructField("Beat", IntegerType(), False),
                                StructField("District", IntegerType(), False),
                                StructField("Ward", IntegerType(), False),
                                StructField("Community Area", IntegerType(), False),
                                StructField("FBI Code", StringType(), False ),
                                StructField("X Coordinate", FloatType(), False),
                                StructField("Y Coordinate", FloatType(), False ),
                                StructField("Year", IntegerType(), False),
                                StructField("Updated On", DateType(), False ),
                                StructField("Latitude", FloatType(), False),
                                StructField("Longitude", FloatType(), False),
                                StructField("Location", StringType(), False )])
    data = spark.read.option("header", "True").option("schema", crimes_schema).csv(path).na.drop()
    return spark,data

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
        f.write(item)
    f.close()

def save_Df_To_Csv(df, filePath):
    df.repartition(1).write.csv(filePath, sep='|');

def lists_to_csv(yearList, yesList, noList, filePath):
 
    dict = {'year': yearList, 'yesArrest': yesList, 'noArrest': noList}  
    df = pd.DataFrame(dict) 
    df.to_csv(filePath) 
