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
from pyspark.ml.classification import LinearSVC
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

#Regression algorithms
from pyspark.ml.regression import LinearRegression
from pyspark.ml.regression import GeneralizedLinearRegression
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.regression import AFTSurvivalRegression
from pyspark.ml.evaluation import RegressionEvaluator


class Evaluate:
    """ A class for resampling and evaluation """


    def __init__(self, definer, preparer, featurer):
        self.definer = definer
        self.preparer = preparer
        self.featurer = featurer

        if definer is not None:
            self.problem_type = definer.problem_type
        self.plot_html = None

        self.report = None
        self.raw_report = None
        self.best_pipelines = None
        self.pipelines = None
        self.estimators = None
        # self.X_train = None
        # self.y_train = None
        # self.X_test = None
        # self.y_test = None
        # self.y_pred = None
        self.train = None
        self.test = None

        self.metrics = dict()
        self.feature_importance = dict()

        self.test_size = 0.3
        self.num_folds = 10
        self.seed = 7

    def pipeline(self):

        #evaluators = []
        self.build_pipelines(self.set_models())
        self.evaluate_pipelines()
        #self.setBestPipelines()

        #[m() for m in evaluators]
        return self

    def set_models(self):

        models = []

        if self.problem_type == 'classification':

            #Classification algorithms
            models.append(('LogisticRegression', LogisticRegression(labelCol=self.definer.response, \
                                                                    featuresCol='scaledFeatures')))
            models.append(('RandomForestClassifier', RandomForestClassifier(labelCol=self.definer.response, \
                                                                            featuresCol='scaledFeatures')))

            models.append(('DecisionTreeClassifier', DecisionTreeClassifier(labelCol=self.definer.response, \
                                                                            featuresCol='scaledFeatures')))
            models.append(('GBTClassifier', GBTClassifier(labelCol=self.definer.response, \
                                                                            featuresCol='scaledFeatures')))
            models.append(('LinearSVC', LinearSVC(labelCol=self.definer.response, \
                                                                   featuresCol='scaledFeatures')))
            # models.append(('RandomForestClassifier', MultilayerPerceptronClassifier(labelCol=self.definer.response, \
            #                                                        featuresCol='scaledFeatures')))
            # models.append(('RandomForestClassifier', NaiveBayes(labelCol=self.definer.response, \
            #                                                                         featuresCol='scaledFeatures')))
        else:

            #Regression algorithms
            models.append(('LinearRegression', LinearRegression(labelCol=self.definer.response,\
                                                               featuresCol='scaledFeatures')))
            models.append(('GeneralizedLinearRegression', GeneralizedLinearRegression(labelCol=self.definer.response,\
                                                                                     featuresCol='scaledFeatures')))
            models.append(('DecisionTreeRegressor', DecisionTreeRegressor(labelCol=self.definer.response, \
                                                                         featuresCol='scaledFeatures')))
            models.append(('RandomForestRegressor', RandomForestRegressor(labelCol=self.definer.response, \
                                                                         featuresCol='scaledFeatures')))
            models.append(('GBTRegressor', GBTRegressor(labelCol=self.definer.response, \
                                                       featuresCol='scaledFeatures')))
            models.append(('AFTSurvivalRegression', AFTSurvivalRegression(labelCol=self.definer.response, \
                                                                         featuresCol='scaledFeatures')))

        return models

    def split_data(self, test_size=0.33, seed=7):
        """ Need to fill """

        train, test = self.definer.data.randomSplit([1-test_size, test_size], seed=seed)
        
        self.train = train
        self.test = test

    def build_pipelines(self, models):
        pipelines = []

        for m in models:
            chains = self.preparer + self.featurer + [m[1]]
            pipelines.append((m[0], Pipeline(stages=chains)))

        self.pipelines = pipelines

    def evaluate_pipelines(self):

        test_size = self.test_size
        num_folds = self.num_folds
        seed = self.seed
        if self.definer.problem_type == 'classification':
            metric_name = 'accuracy'
        else:
            metric_name = 'r2'

        self.split_data(test_size, seed)

        report = [["Model", "Score", "Time"]]
        names = []
        total_time = 0.0

        for name, pipeline in self.pipelines:
            print("Model: ", name)
            if self.definer.problem_type == 'classification':
                evaluator = MulticlassClassificationEvaluator(labelCol=self.definer.response,
                                                              predictionCol="prediction",
                                                              metricName=metric_name)
            else:
                evaluator = RegressionEvaluator(labelCol=self.definer.response,
                                                predictionCol="prediction",
                                                metricName=metric_name)

            paramGrid = ParamGridBuilder().build()
    
            start = time.time()
            crossval = CrossValidator(estimator=pipeline,\
                                      estimatorParamMaps=paramGrid,\
                                      evaluator=evaluator,\
                                      numFolds=num_folds)
            
            cvModel = crossval.fit(self.train)
            end = time.time()
            prediction = cvModel.transform(self.test)
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
             
        self.report = report.sort(col("Score").desc())
        #self.report.write.overwrite().csv('hdfs://King:9000/user/bdata/cern/report.csv', header=True)
        
        self.report.show(truncate=False)

    def setBestPipelines(self):
        alg = list(self.report.Model)[0:2]
        best_pipelines = []

        for p in self.pipelines:
            if p[0] in alg:
                best_pipelines.append(p)

        self.best_pipelines = best_pipelines

        #print(self.best_pipelines)

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
