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
    bestPipelines = None
    pipelines = None
    train = None
    test = None


    def __init__(self, definer, preparer, featurer):
        self.definer = definer
        self.preparer = preparer
        self.featurer = featurer


    def pipeline(self):

        #evaluators = []
        self.buildPipelines(self.defineAlgorithms())
        self.evaluatePipelines()
        #self.setBestPipelines()

        #[m() for m in evaluators]
        return self

    def defineAlgorithms(self):

        models = []

        #Regression algorithms
        #models.append(('LinearRegression', LinearRegression(labelCol=self.definer.className,\
        #                                                    featuresCol='scaledFeatures')))
        models.append(('GeneralizedLinearRegression', GeneralizedLinearRegression(labelCol=self.definer.className,\
                                                                                  featuresCol='scaledFeatures')))
        models.append(('DecisionTreeRegressor', DecisionTreeRegressor(labelCol=self.definer.className, \
        #                                                              featuresCol='scaledFeatures')))
        #models.append(('RandomForestRegressor', RandomForestRegressor(labelCol=self.definer.className, \
        #                                                              featuresCol='scaledFeatures')))
        #models.append(('GBTRegressor', GBTRegressor(labelCol=self.definer.className, \
        #                                            featuresCol='scaledFeatures')))
        #models.append(('AFTSurvivalRegression', AFTSurvivalRegression(labelCol=self.definer.className, \
        #                                                              featuresCol='scaledFeatures')))
       

        return models

    def defineTrainingData(self, test_size=0.33, seed=7):
        """ Need to fill """

        train, test = self.definer.data.randomSplit([1-test_size, test_size], seed=seed)
        
        Evaluate.train = train
        Evaluate.test = test

    def buildPipelines(self, models):
        pipelines = []

        for m in models:
            chains = self.preparer + self.featurer + [m[1]]
            pipelines.append((m[0], Pipeline(stages=chains)))

        Evaluate.pipelines = pipelines

    def evaluatePipelines(self, ax=None):

        test_size = 0.2
        num_folds = 10
        seed = 7
        score = "r2"

        self.defineTrainingData(test_size, seed)

        report = [["Model", "Score", "Time"]]
        names = []
        total_time = 0.0

        for name, pipeline in Evaluate.pipelines:
            
            evaluator = RegressionEvaluator(labelCol=self.definer.className, predictionCol="prediction", metricName=score)
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
            total_time += duration
            # save the model to disk
            #filename = name+'.ml'
            #pickle.dump(model, open('./models/'+filename, 'wb'))
    
            names.append(name)
            report.append([name, metric, duration])
            #print(report_print)

        
        report.append(['Total time', 0.0, total_time])
        headers = report.pop(0)
        df_report = self.definer.sparkSession.createDataFrame(report, headers)
        self.chooseTopRanked(df_report)
        #self.plotModels(results, names)


    def chooseTopRanked(self, report):
        """" Sort the models by its score"""
             
        Evaluate.report = report.sort(col("Score").desc())
        #Evaluate.report.write.csv("output")
        
        Evaluate.report.show(truncate=False)

    def setBestPipelines(self):
        alg = list(Evaluate.report.Model)[0:2]
        bestPipelines = []

        for p in Evaluate.pipelines:
            if p[0] in alg:
                bestPipelines.append(p)

        Evaluate.bestPipelines = bestPipelines

        #print(Evaluate.bestPipelines)

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
