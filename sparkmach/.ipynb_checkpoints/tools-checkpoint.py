#!/usr/bin/python
# -*- coding: utf-8 -*-

# Author: Gusseppe Bravo <gbravor@uni.pe>
# License: BSD 3 clause
"""
Helper functions
"""

from pyspark.sql.functions import col, rand, randn, when

def generate_dataframe(spark_session, n_samples, n_features, seed=42):
    '''
        Generate a binary labeled dataframe for classification problems
    '''
    df = spark_session.range(0, n_samples)
    t = [when(df['id'] < n_samples/2, 0).otherwise(1).alias("response")]
    t += [rand(seed=seed+i).alias("f_"+str(i)) for i in range(n_features)]
    df = df.select(t)

    return df