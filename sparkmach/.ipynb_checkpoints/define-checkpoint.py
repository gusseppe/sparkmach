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
from pyspark.sql.functions import col
#from pyspark import SparkContext, SparkConf#version 1.62

        
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

    def __init__(self, sparkSession, nameData=None, header=None, className='class', df=None):
        self.sparkSession = sparkSession
        self.nameData = nameData
        self.header = header
        self.className = className
        self.df = df

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
            #spark = SparkSession \
            #.builder \
            #.appName("Sparkmach") \
            #.config("spark.some.config.option", "some-value") \
            #.getOrCreate()
            if self.df is not None:
                df = self.df.dropna()
                
                #df = df.withColumn("class", col("class").cast('float'))
                #df = df.withColumn("bus_id", col("bus_id").cast('float'))
                #df = df.withColumn("direction", col("direction").cast('float'))
                #df = df.withColumn("half_hour_bucket", col("half_hour_bucket").cast('float'))
                
                
                Define.data = df.dropna()
                
                if self.header is not None:
                    Define.header = self.header


                Define.X = Define.data.drop(self.className)#.show()
                Define.y = Define.data.select(self.className)
                
            elif self.nameData is not None and self.className is not None: 
                df = self.sparkSession.read\
                .format("csv")\
                .option("header", "true")\
                .option("mode", "DROPMALFORMED")\
                .option("inferSchema", "true")\
                .csv(self.nameData)

                df = df.dropna()
                Define.data = df
                
                if self.header is not None:
                    Define.header = self.header


                Define.X = Define.data.drop(self.className)#.show()
                Define.y = Define.data.select(self.className)
                
                
        except:
            print ("Error reading")        

    def description(self):
        Define.n_features = len(Define.X.columns)
        Define.samples = Define.data.count()

    #def likelyAlgorithms(self):
        #if Define.samples < 50:
            #print("Not enough data")
        #else:
            #pass