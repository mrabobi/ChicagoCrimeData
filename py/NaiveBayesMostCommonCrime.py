from CrimeStatisticsMain import *
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import NaiveBayes
from pyspark.ml import Pipeline
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
(trainingData, testData) = data.randomSplit([0.7, 0.3], seed = 100)
trainingData.cache()
testData.cache()
bayes = NaiveBayes(smoothing=1.0, modelType="multinomial")
pipeline = Pipeline(stages=[vecAssembler, bayes])
model = pipeline.fit(trainingData)
predictions = model.transform(testData)

evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Model Accuracy: "+str(accuracy))