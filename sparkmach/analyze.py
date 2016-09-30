#!/usr/bin/python
# -*- coding: utf-8 -*-

# Author: Gusseppe Bravo <gbravor@uni.pe>
# License: BSD 3 clause
"""
This module provides a few of useful functions (actually, methods)
for describing the dataset which is to be studied.

"""
from __future__ import print_function
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd


from pandas.tools.plotting import scatter_matrix
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf

#try:
#    sc.stop()
#except:
#    pass


__all__ = [
    'read', 'description', 'classBalance', 'hist', 'density']


class Analyze():
    """ A class for data analysis """

    data = None
    X = None
    y = None

    def __init__(self, definer, sparkcontext):
        """The init class.

        Parameters
        ----------
        typeModel : string
            String that indicates if the model will be train for clasification
            or regression.
        className : string
            String that indicates which column in the dataset is the class.

        """
        self.typeModel = definer.typeModel
        self.typeAlgorithm = definer.typeAlgorithm
        self.className = definer.className
        self.nameData = definer.nameData
        self.sparkcontext = sparkcontext

    def pipeline(self):

        analyzers = []
        analyzers.append(self.read)
        analyzers.append(self.hist)
        analyzers.append(self.density)
        analyzers.append(self.corr)
        analyzers.append(self.scatter)

        [m() for m in analyzers]


    def read(self):
        """Read the dataset.

        Returns
        -------
        out : ndarray

        """
        #config = SparkConf()
        #sc = SparkContext()
        sqlContext = SQLContext(self.sparkcontext)
        pdf = pd.read_csv(self.nameData)
        Analyze.data = sqlContext.createDataFrame(pdf)
        #Analyze.data = data.ix[:, data.columns != self.className]
        Analyze.X = Analyze.data.drop(self.className)#.show()
        Analyze.y = Analyze.data.select(self.className)
        #return Analyze.X

    def description(self):
        """Shows a basic data description .

        Returns
        -------
        out : ndarray

        """
        return Analyze.X.describe()

    def classBalance(self):
        """Shows how balanced the class values are.

        Returns
        -------
        out : pandas.core.series.Series
        Serie showing the count of classes.

        """
        return Analyze.y.groupby(self.className).count()

    def hist(self):
        
        Analyze.data.toPandas().hist(color=[(0.196, 0.694, 0.823)]) 
        plt.show()

    def density(self):
        #Analyze.data.plot(color=[(0.196, 0.694, 0.823)], kind='density', 
                #subplots=True, layout=(3,3), sharex=False, figsize = (10, 10)) 
        Analyze.data.toPandas().plot(kind='density', 
                subplots=True, layout=(3,3), sharex=False) 
        plt.show()

    def corr(self):
        corr = Analyze.data.toPandas().corr()
        fig, ax = plt.subplots()
        bar = ax.matshow(corr, vmin=-1, vmax=1)
        fig.colorbar(bar)
        plt.xticks(range(len(corr.columns)), corr.columns);
        plt.yticks(range(len(corr.columns)), corr.columns);
        plt.show()

    def scatter(self):
        pdf = Analyze.data.toPandas()
        scatter_matrix(pdf, alpha=0.7, figsize=(6, 6), diagonal='kde')
        plt.show()

