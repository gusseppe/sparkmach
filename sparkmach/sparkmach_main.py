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

name = "datasets/buses_10000_filtered.csv"
#     name = "hdfs://King:9000/user/bdata/mta_data/MTA-Bus-Time_.2014-08-01.txt"
className = "class"


sparkSession = SparkSession.builder \
.master('local[*]')\
.appName("Sparkmach") \
.config("spark.driver.allowMultipleContexts", "true")\
.getOrCreate()

currentDir = os.getcwd()
#Piero
sparkSession.sparkContext.addPyFile(currentDir + "/bus_times.py") 

#Gusseppe
sparkSession.sparkContext.addPyFile(currentDir + "/define.py")
sparkSession.sparkContext.addPyFile(currentDir + "/prepare.py")
sparkSession.sparkContext.addPyFile(currentDir + "/feature_selection.py")
sparkSession.sparkContext.addPyFile(currentDir + "/evaluate.py")

rdd = sparkSession.sparkContext.textFile(currentDir + '/datasets/MTA-Bus-Time_.2014-08-01.txt')
# rdd = sparkSession.sparkContext.textFile(currentDir + '/datasets/test.txt')
# rdd = sc.textFile('hdfs://King:9000/user/bdata/mta_data/MTA-Bus-Time_.2014-10-31.txt')


classTuple= bus_times.mainFilter(rdd)
halfHourBucket=classTuple.map(lambda x: bus_times.toHalfHourBucket(list(x)))


bucket_schema= StructType([StructField("bus_id",StringType(), True),StructField("route_id",StringType(), True),StructField("next_stop_id",StringType(), True),StructField("direction",StringType(), True),StructField("half_hour_bucket",StringType(), True),StructField("class",StringType(), True) ])
# bucket_schema= StructType([StructField("bus_id",IntegerType(), True),StructField("route_id",StringType(), True),StructField("next_stop_id",StringType(), True),StructField("direction",IntegerType(), True),StructField("half_hour_bucket",FloatType(), True),StructField("class",FloatType(), True) ])

df = sparkSession.createDataFrame(halfHourBucket, bucket_schema)
stringIndexer = StringIndexer(inputCol='route_id', outputCol='route_id'+"_Index")
df = stringIndexer.fit(df).transform(df)    

stringIndexer = StringIndexer(inputCol='next_stop_id', outputCol='next_stop_id'+"_Index")
df = stringIndexer.fit(df).transform(df)    
drop_list = ['route_id', 'next_stop_id']

df = df.select([column for column in df.columns if column not in drop_list])


# print('hellllooo')

# STEP 0: Define workflow parameters
definer = define.Define(sparkSession, nameData=name, className=className, df=df).pipeline()

# STEP 1: Analyze data by ploting it
#analyze.Analyze(definer).pipeline()

# STEP 2: Prepare data by scaling, normalizing, etc. 
preparer = prepare.Prepare(definer).pipeline()

#STEP 3: Feature selection
featurer = feature_selection.FeatureSelection(definer).pipeline()

#STEP4: Evalute the algorithms by using the pipelines
evaluator = evaluate.Evaluate(definer, preparer, featurer).pipeline()



