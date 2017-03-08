#!/usr/bin/python
# -*- coding: utf-8 -*-

# Author: Gusseppe Bravo <gbravor@uni.pe>
# License: BSD 3 clause
"""
En esta clase se define que problema se va a solucionar.
Sea de clasificacion, regression, clustering. Ademas se debe
dar una idea de los posibles algoritmos que pueden ser usados.
"""

import pandas as pd
from pyspark.sql import SQLContext
#from pyspark import SparkContext, SparkConf#version 1.62
from pyspark.sql import SparkSession
        
#try:
#    from pyspark.ml.linalg import Vectors#Version 2                    
#except ImportError:
#    from pyspark.mllib.linalg import Vectors#Version 1.62

class Define():

    typeModel = 'clasification'
    typeAlgorithm = 'LogisticR'
    n_features = None
    samples = None
    data = None
    header = None
    X = None
    y = None

    def __init__(self, nameData=None, header=None, className=None):
        #self.sparkcontext = sparkcontext
        self.nameData = nameData
        self.header = header
        self.className = className

    def pipeline(self):

        definers = []
        definers.append(self.read)
        #definers.append(self.toVectors)
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
            #sqlContext = SQLContext(self.sparkcontext)#version 1.62
            spark = SparkSession \
            .builder \
            .appName("Sparkmach") \
            .config("spark.some.config.option", "some-value") \
            .getOrCreate()
            
            if self.nameData is not None and self.className is not None: 
                if self.header is not None:

                    pdf = pd.read_csv(self.nameData, names=self.header)
                    pdf.dropna(inplace=True)#Future optimization by using DF API.
                    #Define.data = sqlContext.createDataFrame(pdf)#version 1.62
                    Define.data = spark.createDataFrame(pdf)
                    Define.header = self.header

                else:    
                    pdf = pd.read_csv(self.nameData)
                    #Define.data = sqlContext.createDataFrame(pdf)#version 1.62
                    Define.data = spark.createDataFrame(pdf)

                Define.X = Define.data.drop(self.className)#.show()
                Define.y = Define.data.select(self.className)
                
                
        except:
            print "Error reading"        

    def description(self):
        Define.n_features = len(Define.X.columns)
        Define.samples = Define.data.count()

    #def likelyAlgorithms(self):
        #if Define.samples < 50:
            #print("Not enough data")
        #else:
            #pass