#!/usr/bin/python
# -*- coding: utf-8 -*-

# Author: Gusseppe Bravo <gbravor@uni.pe>
# License: BSD 3 clause

"""
This module provides the logic of the whole project.

"""
import define
#import analyze
import prepare
import feature_selection
import evaluate

import time
import os

from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
#from pyspark import SparkContext, SparkConf

try:
#     spark.stop()
    pass
except:
    pass
    
#name = "datasets/buses_10000_filtered.csv"
name = "hdfs://King:9000/user/bdata/buses_10000_filtered.csv"
className = "tiempoRecorrido"

sparkSession = SparkSession.builder \
.master('spark://King:7077') \
.appName("Sparkmach") \
.config("spark.driver.allowMultipleContexts", "true")\
.getOrCreate()
    
    
# conf = SparkConf()\
# .setMaster("local")\
# .setAppName("sparkmach")\
# .set("spark.driver.allowMultipleContexts", "true")

#sparkContext = SparkContext(conf=conf)

currentDir = os.getcwd()
sparkSession.sparkContext.addPyFile(currentDir + "/define.py")
#sparkSession.sparkContext.addPyFile("/home/vagrant/tesis/sparkmach/sparkmach/sparkmach/analyze.py")
sparkSession.sparkContext.addPyFile(currentDir + "/prepare.py")
sparkSession.sparkContext.addPyFile(currentDir + "/feature_selection.py")
sparkSession.sparkContext.addPyFile(currentDir + "/evaluate.py")

# STEP 0: Define workflow parameters
definer = define.Define(sparkSession, nameData=name, className=className).pipeline()

# STEP 1: Analyze data by ploting it
# analyze.Analyze(definer).pipeline()

# STEP 2: Prepare data by scaling, normalizing, etc. 
preparer = prepare.Prepare(definer).pipeline()

#STEP 3: Feature selection
featurer = feature_selection.FeatureSelection(definer).pipeline()

#STEP4: Evalute the algorithms by using the pipelines
evaluator = evaluate.Evaluate(definer, preparer, featurer).pipeline()


# start = time.time()
# result = main()
# end = time.time()

# print()
# print("Execution time for all the steps: ", end-start)