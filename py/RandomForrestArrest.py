from CrimeStatisticsMain import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import RandomForestClassifier



if len(sys.argv)<2:
    print("Please provide an input file")
    exit()
    
spark,data=load_dataframe(sys.argv[1],"Naive Bayes Primary Crim Type Arrest")
data=prepare_data_for_feature_vector(data,spark)
data=convert_arrest_to_label_column(data)

vecAssembler = VectorAssembler(inputCols=["PrimaryType","Domestic","Beat","District","Ward","CommunityArea","Year"], outputCol="features")
data = vecAssembler.transform(data)
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

#spark-submit RandomForrestArrest.py Crimes.csv

