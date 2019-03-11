#!/usr/bin/python
# -*- coding: utf-8 -*-

# Author: Gusseppe Bravo <gbravor@uni.pe>
# License: BSD 3 clause
"""
En esta clase se define que problema se va a solucionar.
Sea de clasificacion, regression, clustering. Ademas se debe
dar una idea de los posibles algoritmos que pueden ser usados.
"""


from pyspark.sql import SQLContext
from pyspark.sql.functions import col, rand, randn, when
#from pyspark import SparkContext, SparkConf#version 1.62

        
#try:
#    from pyspark.ml.linalg import Vectors#Version 2                    
#except ImportError:
#    from pyspark.mllib.linalg import Vectors#Version 1.62

class Define:

    def __init__(self,
            spark_session,
            data_path=None,
            df=None,
            header=None,
            response='class',
            num_features=None,
            cat_features=None,
            problem_type='classification'):

        self.spark_session = spark_session
        self.data_path = data_path
        self.df = df
        self.header = header
        self.response = response
        self.metadata = dict()

        self.problem_type = problem_type
        self.infer_algorithm = 'LogisticR'
        self.n_features = None
        self.num_features = num_features
        self.cat_features = cat_features
        self.samples = None
        self.size = None
        self.data = None
        self.X = None
        self.y = None

    def pipeline(self):

        definers = list()
        definers.append(self.read)
        definers.append(self.description)

        [m() for m in definers]

        return self

    def read(self):
        """Read the dataset.

        Returns
        -------
        out : ndarray

        """
        try:   
            if self.df is not None:
                df = self.df.dropna()
                              
                self.data = df.dropna()
                
                if self.header is not None:
                    self.header = self.header

                self.X = self.data.drop(self.response)#.show()
                self.y = self.data.select(self.response)
                
            elif self.data_path is not None and self.response is not None:
                df = self.spark_session.read\
                    .format("csv")\
                    .option("header", "true")\
                    .option("mode", "DROPMALFORMED")\
                    .option("inferSchema", "true")\
                    .csv(self.data_path)

                df = df.dropna()
                self.data = df
                
                if self.header is not None:
                    self.header = self.header

                self.X = self.data.drop(self.response)#.show()
                self.y = self.data.select(self.response)

        except Exception as e:

            print("Error reading  | ", e)

    def description(self):
        self.n_features = len(self.X.columns)
        self.samples = self.data.count()
    
    #def likelyAlgorithms(self):
        #if self.samples < 50:
            #print("Not enough data")
        #else:
            #pass
