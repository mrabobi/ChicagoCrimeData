import sys
from CrimeStatisticsMain import *
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import NaiveBayes
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer


if len(sys.argv)<2:
    print("Please provide an input file")
    exit()

spark,data=load_dataframe(sys.argv[1],"Naive Bayes Primary Crime Type")
data=prepare_data_for_feature_vector(data,spark,includeArrest=True)
labelIndexer = StringIndexer(inputCol="PrimaryType", outputCol="label")
(trainingData, testData) = data.randomSplit([0.7, 0.3], seed = 100)
trainingData.cache()
testData.cache()

vecAssembler = VectorAssembler(inputCols=["Arrest","Domestic","Beat","District","Ward","CommunityArea","Year"], outputCol="features")
bayes = NaiveBayes(smoothing=1.0, modelType="multinomial")
pipeline = Pipeline(stages=[labelIndexer,vecAssembler, bayes])
model = pipeline.fit(trainingData)
predictions = model.transform(testData)

#predictions.select("label", "prediction").show()
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Model Accuracy: "+str(accuracy))

#spark-submit NaiveBayesPrimaryType.py Crimes.csv
