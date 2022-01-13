from CrimeStatisticsMain import *
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import NaiveBayes
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator



if len(sys.argv)<2:
    print("Please provide an input file")
    exit()
    
spark,data=load_dataframe(sys.argv[1],"Naive Bayes Primary Crim Type Arrest")
data=prepare_data_for_feature_vector(data,spark)
data=convert_arrest_to_label_column(data)
(trainingData, testData) = data.randomSplit([0.7, 0.3], seed = 100)
trainingData.cache()
testData.cache()

vecAssembler = VectorAssembler(inputCols=["PrimaryType","Domestic","Beat","District","Ward","CommunityArea","Year"], outputCol="features")
bayes = NaiveBayes(smoothing=1.0, modelType="multinomial")
pipeline = Pipeline(stages=[vecAssembler, bayes])
model = pipeline.fit(trainingData)
predictions = model.transform(testData)

#predictions.select("label", "prediction").show()
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Model Accuracy: "+str(accuracy))

#spark-submit NaiveBayesArrest.py Crimes.csv
