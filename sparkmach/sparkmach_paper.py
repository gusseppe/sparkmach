import bus_times
import os
import define
#import analyze
import prepare
import feature_selection
import evaluate


from pyspark.ml.feature import StringIndexer
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row, SparkSession
from pyspark.sql.types import *

#name = "datasets/buses_10000_filtered.csv"
name = "hdfs://King:9000/user/bdata/cern/hepmass_2000000_report.csv"
response = "label"
#master = 'local[*]''
master_name = 'King:9000'

spark_session = SparkSession.builder \
.master(master_name)\
.appName("Sparkmach") \
.config("spark.driver.allowMultipleContexts", "true")\
.getOrCreate()

currentDir = os.getcwd()

#Gusseppe
spark_session.sparkContext.addPyFile(currentDir + "/define.py")
spark_session.sparkContext.addPyFile(currentDir + "/prepare.py")
spark_session.sparkContext.addPyFile(currentDir + "/feature_selection.py")
spark_session.sparkContext.addPyFile(currentDir + "/evaluate.py")


# print('hellllooo')

# STEP 0: Define workflow parameters
definer = define.Define(spark_session, data_path=name, response=response, df=df).pipeline()

# STEP 1: Analyze data by ploting it
#analyze.Analyze(definer).pipeline()

# STEP 2: Prepare data by scaling, normalizing, etc. 
preparer = prepare.Prepare(definer).pipeline()

#STEP 3: Feature selection
featurer = feature_selection.FeatureSelection(definer).pipeline()

#STEP4: Evalute the algorithms by using the pipelines
evaluator = evaluate.Evaluate(definer, preparer, featurer).pipeline()



