#!/usr/bin/python
# -*- coding: utf-8 -*-

# Author: Gusseppe Bravo <gbravor@uni.pe>
# License: BSD 3 clause
"""
This module provides ideas for evaluating some machine learning algorithms.

"""
from __future__ import print_function
import time
#import numpy as np
#import pandas as pd
#import operator
#import matplotlib.pyplot as plt
#import warnings
#import pickle
#sklearn warning
#warnings.filterwarnings("ignore", category=DeprecationWarning)

from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql.functions import col


#Clasification algorithms
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

#Regression algorithms
from pyspark.ml.regression import LinearRegression
from pyspark.ml.regression import GeneralizedLinearRegression
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.regression import AFTSurvivalRegression
from pyspark.ml.evaluation import RegressionEvaluator



class Evaluate():
    """ A class for resampling and evaluation """

    report = None
    best_pipelines = None
    pipelines = None
    train = None
    test = None


    def __init__(self, definer, preparer, featurer):
        self.definer = definer
        self.preparer = preparer
        self.featurer = featurer


    def pipeline(self):

        #evaluators = []
        self.build_pipelines(self.set_models())
        self.evaluate_pipelines()
        #self.setBestPipelines()

        #[m() for m in evaluators]
        return self

    def set_models(self):

        models = []

        #Regression algorithms
        #models.append(('LinearRegression', LinearRegression(labelCol=self.definer.response,\
        #                                                    featuresCol='scaledFeatures')))
        #models.append(('GeneralizedLinearRegression', GeneralizedLinearRegression(labelCol=self.definer.response,\
        #                                                                          featuresCol='scaledFeatures')))
        #models.append(('DecisionTreeRegressor', DecisionTreeRegressor(labelCol=self.definer.response, \
        #                                                              featuresCol='scaledFeatures')))
        #models.append(('RandomForestRegressor', RandomForestRegressor(labelCol=self.definer.response, \
        #                                                              featuresCol='scaledFeatures')))
        #models.append(('GBTRegressor', GBTRegressor(labelCol=self.definer.response, \
        #                                            featuresCol='scaledFeatures')))
        #models.append(('AFTSurvivalRegression', AFTSurvivalRegression(labelCol=self.definer.response, \
        #                                                              featuresCol='scaledFeatures')))
        
        #Classification algorithms
        models.append(('LogisticRegression', LogisticRegression(labelCol=self.definer.response,\
                                                            featuresCol='scaledFeatures')))
        models.append(('RandomForestClassifier', RandomForestClassifier(labelCol=self.definer.response,\
                                                                                  featuresCol='scaledFeatures')))
       

        return models

    def split_data(self, test_size=0.33, seed=7):
        """ Need to fill """

        train, test = self.definer.data.randomSplit([1-test_size, test_size], seed=seed)
        
        Evaluate.train = train
        Evaluate.test = test

    def build_pipelines(self, models):
        pipelines = []

        for m in models:
            chains = self.preparer + self.featurer + [m[1]]
            pipelines.append((m[0], Pipeline(stages=chains)))

        Evaluate.pipelines = pipelines

    def evaluate_pipelines(self, ax=None):

        test_size = 0.2
        num_folds = 10
        seed = 7
        score = "accuracy"

        #score = "r2"
        
        self.split_data(test_size, seed)

        report = [["Model", "Score", "Time"]]
        names = []
        total_time = 0.0

        for name, pipeline in Evaluate.pipelines:
            

            #evaluator = RegressionEvaluator(labelCol=self.definer.response, predictionCol="prediction", metricName=score)
            evaluator = MulticlassClassificationEvaluator(labelCol=self.definer.response, predictionCol="prediction", metricName=score)
            paramGrid = ParamGridBuilder()\
            .build()
    
            start = time.time()
            crossval = CrossValidator(estimator=pipeline,\
                          estimatorParamMaps=paramGrid,\
                          evaluator=evaluator,\
                          numFolds=num_folds)
            
            
            cvModel = crossval.fit(Evaluate.train)
            end = time.time()
            prediction = cvModel.transform(Evaluate.test)
            metric = evaluator.evaluate(prediction)
            duration = end-start
            total_time += duration / 60.0
            # save the model to disk
            filename = name+'.ml'
            
            #cvModel.bestModel.save("hdfs://King:9000/user/bdata/mta_pipelines/"+filename)
            total_time += duration
            # save the model to disk
            filename = name+'.ml'
            #cvModel.bestModel.save('./models/'+filename)
            #pickle.dump(model, open('./models/'+filename, 'wb'))
    
            names.append(name)
            report.append([name, metric, duration/60.0])

        
        report.append(['Total time', 0.0, total_time/60.0])
        headers = report.pop(0)
        df_report = self.definer.spark_session.createDataFrame(report, headers)
        self.chooseTopRanked(df_report)
        #self.plotModels(results, names)


    def chooseTopRanked(self, report):
        """" Sort the models by its score"""
             
        Evaluate.report = report.sort(col("Score").desc())
        #Evaluate.report.write.overwrite().csv('hdfs://King:9000/user/bdata/cern/report.csv', header=True)
        
        Evaluate.report.show(truncate=False)

    def setBestPipelines(self):
        alg = list(Evaluate.report.Model)[0:2]
        best_pipelines = []

        for p in Evaluate.pipelines:
            if p[0] in alg:
                best_pipelines.append(p)

        Evaluate.best_pipelines = best_pipelines

        #print(Evaluate.best_pipelines)

    def plotModels(self, results, names):
        """" Plot the best two algorithms by using box plots"""

        fig = plt.figure()
        fig.suptitle("Model Comparison")
        #ax1 = fig.add_subplot(111)
        ax = fig.add_subplot(111)
        ax.set_xticklabels(names)
        plt.boxplot(results)
        #ax1.set_xticklabels(names)
        #plt.show()
