#!/usr/bin/python
# -*- coding: utf-8 -*-

# Author: Gusseppe Bravo <gbravor@uni.pe>
# License: BSD 3 clause

"""
This module provides the logic of the whole project.

"""
import define
import analyze
import prepare
import feature_selection
import evaluate

import time

from pyspark.ml import Pipeline
from pyspark.sql import SparkSession


def main():
    
    name = "datasets/buses_10000_filtered.csv"
#     name = "hdfs://King:9000/user/bdata/mta_data/MTA-Bus-Time_.2014-08-01.txt"
    className = "tiempoRecorrido"
    
    spark = SparkSession \
    .builder \
    .master('local')\
    .appName("Sparkmach") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    
    
    
    #STEP 0: Define workflow parameters
    definer = define.Define(spark, nameData=name, className=className).pipeline()

    #STEP 1: Analyze data by ploting it
    #analyze.Analyze(definer).pipeline()

    #STEP 2: Prepare data by scaling, normalizing, etc. 
    preparer = prepare.Prepare(definer).pipeline()

    #STEP 3: Feature selection
    featurer = feature_selection.FeatureSelection(definer).pipeline()

    #STEP4: Evalute the algorithms by using the pipelines
    #evaluator = evaluate.Evaluate(definer, preparer, featurer).pipeline()
    
    chains = preparer + featurer
    pip = Pipeline(stages=chains)
    result = pip.fit(definer.data).transform(definer.data)
    
    return result

if __name__ == '__main__':
    start = time.time()
    result = main()
    end = time.time()

    print()
    print("Execution time for all the steps: ", end-start)