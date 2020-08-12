#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Aug 12 11:26:30 2020

@author: quangkhanh
"""


import findspark
findspark.init()

import src.read_utils as util

from datetime import datetime

from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, explode, col, monotonically_increasing_id 
from pyspark.sql import Row, Column

def get_spark(appName = "Job description", master  = "local"):  
    conf = SparkConf().setAppName(appName) \
                      .setMaster(master)
    sc = SparkContext.getOrCreate(conf=conf) 
    sqlContext = SQLContext(sc) 
    spark = sqlContext.sparkSession
    return sc, spark 

def read_file(path, format = 'csv'): 
    df = spark.read.option('header', True)\
                .option('sep', ',')\
                .option('multiLine', True) \
                .option('path', path)\
                .option('quote', '"') \
                .option('escape', '"') \
                .format(format)\
                .load()
    return df

def read_industry(df): 
    ''' 
    Read the domains of jobs in sectors, seperate them  
    
    Returns 
    -------
        industries_df: pyspark DataFrame of industries
    '''
    
    industries = df.select('sectors').rdd
    industries = industries.flatMap(util.industries_to_set).distinct().collect()
    rdd = sc.parallelize(industries) 
    industries_rdd = rdd.map(lambda x: Row(industry = x, description=''))
    industries_df = spark.createDataFrame(industries_rdd) 
    return industries_df.select('industry', 'description')


def read_job(df): 
    def clean_job(title): 
        # clean job
        return title 
    
    jobs = df.select('title').rdd.map(clean_job).distinct().collect()
    rdd = sc.parallelize(jobs) 
    job_rdd = rdd.map(lambda x: Row(name_job=x[0], description='', fact=''))
    job_df = spark.createDataFrame(job_rdd) 
    return job_df.select('name_job', 'description', 'fact') 

def read_time(df): 
    time = df.select('timestampISODate').rdd \
                    .map(util.time_to_quarter).distinct().collect()
    rdd = sc.parallelize(time)
    time_rdd = rdd.map(lambda x: Row(day= x[0], month=x[1], 
                                     quarter=x[2], year=x[3]))
    time_df = spark.createDataFrame(time_rdd) 
    return time_df





if __name__ == '__main__': 
    sc, spark = get_spark() 
    df = read_file('data/job_description.csv')
    