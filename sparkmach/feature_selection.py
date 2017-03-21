#!/usr/bin/python
# -*- coding: utf-8 -*-

# Author: Gusseppe Bravo <gbravor@uni.pe>
# License: BSD 3 clause
"""
This module provides a few of useful functions (actually, methods)
for feature selection the dataset which is to be studied.

"""
from __future__ import print_function
from pyspark.ml import Pipeline
from pyspark.ml import Estimator, Transformer
from pyspark.ml.feature import ChiSqSelector

__all__ = [
    'read']


class FeatureSelection():
    """ A class for feature selection """

    data = None

    def __init__(self, definer):
        self.typeModel = definer.typeModel
        self.typeAlgorithm = definer.typeAlgorithm
        self.className = definer.className
        self.nameData = definer.nameData
        self.n_features = definer.n_features

    def pipeline(self):
        """ This function chooses the best way to find features"""

        transformers = []

        #custom = self.CustomFeature()
        #transformers.append(('custom', custom))
        n_features = int(self.n_features/2)

        chisq = ChiSqSelector(numTopFeatures=n_features, featuresCol="scaledFeatures",
                         outputCol="selectedFeatures", labelCol=self.className+"Index")
        transformers.append(chisq)


        return transformers
        #return Pipeline(stages=transformers)

    class CustomFeature(Estimator, Transformer):
        """ A custome class for featuring """

        def transform(self, X, **transform_params):
            #X = pd.DataFrame(X)
            return X

        def fit(self, X, y=None, **fit_params):
            return self
    #def read(self, name):
        #data = pd.read_csv(name)
        #Prepare.data = data

