import bus_times
import os
import define
#import analyze
import prepare
import feature_selection
import evaluate
import tools

from pyspark.ml.feature import StringIndexer
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row, SparkSession
from pyspark.sql.types import *

#name = "datasets/buses_10000_filtered.csv"
#name = "hdfs://King:9000/user/bdata/cern/hepmass_2000000_report.csv"
#current_path = os.getcwd() # For notebook
current_path = os.path.dirname(os.path.abspath(__file__)) # For files
print(current_path)
data_path = os.path.join(current_path, 'hepmass.csv')
response = "label"
#cluster_manager = 'yarn'
#cluster_manager = 'local[*]'

spark_session = SparkSession.builder \
#.master(cluster_manager)\
.appName("Sparkmach") \
.config("spark.driver.allowMultipleContexts", "true")\
.getOrCreate()


spark_session.sparkContext.addPyFile(os.path.join(current_path, 'define.py'))
spark_session.sparkContext.addPyFile(os.path.join(current_path, 'prepare.py'))
spark_session.sparkContext.addPyFile(os.path.join(current_path, 'feature_selection.py'))
spark_session.sparkContext.addPyFile(os.path.join(current_path, 'evaluate.py'))
spark_session.sparkContext.addPyFile(os.path.join(current_path, 'tools.py'))
##### Benchmark starting #####
print('Benchmark starting:')

list_n_samples = [10, 100, 1000]
list_n_features = [5, 10, 15]
print('List number of samples:', list_n_samples)
print('List number of features:', list_n_features)

for n_samples in list_n_samples:
    for n_features in list_n_features:
        
        ##### Generate the dataframe #####
        print('Generating the binary labeled dataframe: shape:', n_samples, n_features)
        df = tools.generate_dataframe(spark_session, n_samples=n_samples, 
                                      n_features=n_features, seed=42)
        # df.show(3)

        ##### Run the models #####
        print('Running the models')

        # STEP 0: Define workflow parameters
        #definer = define.Define(spark_session, data_path=data_path, response=response).pipeline()
        definer = define.Define(spark_session, df=df, response='response').pipeline()

        # STEP 1: Analyze data by ploting it
        #analyze.Analyze(definer).pipeline()

        # STEP 2: Prepare data by scaling, normalizing, etc. 
        preparer = prepare.Prepare(definer).pipeline()

        #STEP 3: Feature selection
        featurer = feature_selection.FeatureSelection(definer).pipeline()

        #STEP4: Evalute the algorithms by using the pipelines
        evaluator = evaluate.Evaluate(definer, preparer, featurer).pipeline()