from CrimeStatisticsMain import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator



if len(sys.argv)<2:
    print("Please provide an input file")
    exit()

spark,data=load_dataframe(sys.argv[1],"Naive Bayes Most Common Crime Location")
data=get_most_frequet_crimes_by_year_and_month_and_location(data)
data=convert_column_index(data,spark,"MostCommonCrime")
data=convert_column_index(data,spark,"LocationDescription")
data=data.withColumnRenamed("MostCommonCrime","label")
vecAssembler = VectorAssembler(inputCols=["Year","LocationDescription"],outputCol="features")
data=vecAssembler.transform(data)
(trainingData, testData) = data.randomSplit([0.7, 0.3], seed = 100)
trainingData.cache()
testData.cache()
rf = RandomForestClassifier(featuresCol = 'features', labelCol = 'label')
rfModel = rf.fit(trainingData)
predictions = rfModel.transform(testData)
predictions.select("label", "prediction")
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction")
accuracy = evaluator.evaluate(predictions)
print("Model Accuracy: "+str(accuracy))