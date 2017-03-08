#!/usr/bin/python
# -*- coding: utf-8 -*-

# Author: Gusseppe Bravo <gbravor@uni.pe>
# License: BSD 3 clause
"""
This module provides a few of useful functions (actually, methods)
for preparing the dataset which is to be studied.

"""
from __future__ import print_function
import numpy as np
import pandas as pd
from pyspark.ml import Pipeline
from pyspark.ml import Estimator, Transformer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import MinMaxScaler, Normalizer, StandardScaler
from pyspark.ml.feature import StringIndexer

__all__ = [
    'pipeline']


class Prepare():
    """ A class for data preparation """

    data = None
    categoricalData = False

    def __init__(self, definer):
        self.definer = definer
        self.typeModel = definer.typeModel
        self.typeAlgorithm = definer.typeAlgorithm
        self.className = definer.className
        self.nameData = definer.nameData

    def pipeline(self):
        """ This function chooses the best way to scale a data"""

        transformers = []

        #clean = self.Clean()
        #transformers.append(('clean', FunctionTransformer(clean, validate=False)))
        #transformers.append(('clean', clean))

        assembler = VectorAssembler(inputCols=self.definer.X.columns, outputCol="features")
        transformers.append(assembler)

        if self.typeAlgorithm in ["NeuralN", "K-N"]:
            minmax = MinMaxScaler(inputCol="features", outputCol="scaledFeatures")
            normalizer = Normalizer(inputCol="features", outputCol="scaledFeatures")
            transformers.append(minmax)
            transformers.append(normalizer)
        elif self.typeAlgorithm in ["LinearR", "LogisticR"]:
            #scaler = RobustScaler()
            scaler = StandardScaler(inputCol='features', outputCol="scaledFeatures", 
                                    withMean = True, withStd = True)
            transformers.append(scaler)
        else:
            #scaler = StandardScaler()
            scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withMean = True, withStd = True)
            transformers.append(scaler)

        stringIndexer = StringIndexer(inputCol=self.className, outputCol=self.className+"Index")
        transformers.append(stringIndexer)
        
        return transformers
        #return Pipeline(stages=transformers)
        #return FeatureUnion(transformers)

    class Clean(Estimator, Transformer):
        """ A class for removing NAN values """

        def transform(self, X, y=None, **transform_params):
            #X = pd.DataFrame(X)
            X.dropna(inplace=True)
            #return X.dropna()
            return X

        def fit_transform(self, X, y=None, **fit_params):
            self.fit(X, y, **fit_params)
            return self.transform(X)
            
        def fit(self, X, y=None, **fit_params):
            return self

    class CategoricalToNumeric(Estimator, Transformer):
        """ A class for parsing categorical columns """

        def categoricalColumns(self, df):
            cols = df.columns
            cols_numeric = df._get_numeric_data().columns
            return list(set(cols) - set(cols_numeric))

        def categoricalToNumeric(self, df):
            cat_columns = self.categoricalColumns(df)
            if cat_columns:
                Prepare.categoricalData = True
                for category in cat_columns:
                    encoder = LabelEncoder()
                    #df.loc[:, category+'_n'] = encoder.fit_transform(df[category])
                    df.loc[:, category] = encoder.fit_transform(df[category])
                    
            #df.drop(cat_columns, axis=1, inplace=True)
            return df

        def transform(self, X, y=None, **transform_params):
            #X = pd.DataFrame(X)
            return self.categoricalToNumeric(X)
            #return X.dropna()

        def fit_transform(self, X, y=None, **fit_params):
            self.fit(X, y, **fit_params)
            return self.transform(X)
            
        def fit(self, X, y=None, **fit_params):
            return self
