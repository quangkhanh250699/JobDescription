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
    return sc, spark, sqlContext

sc, spark, sqlContext = get_spark()
url = 'jdbc:mysql://localhost:3306/'
db_name = 'test_labor_market_analysis'
user = 'quangkhanh'
password = '12345678'

def read_table(table_name, sqlContext=sqlContext, url=url, db_name=db_name,
                           user=user, password=password): 
    df = sqlContext.read.option('url', url + db_name) \
                        .option('driver', 'com.mysql.jdbc.Driver') \
                        .option('dbtable', table_name) \
                        .option('user', user) \
                        .option('password', password) \
                        .format('jdbc') \
                        .load() 
    return df
    
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
    industries_rdd = rdd.map(lambda x: Row(name_industry = x, description=''))
    industries_df = spark.createDataFrame(industries_rdd) \
                            .withColumn("idIndustry", monotonically_increasing_id())
    return industries_df.select('idIndustry', 'name_industry', 'description') 


def read_job(df): 
    '''
    Read the job in title of job descriptions dataframe

    Parameters
    ----------
    df : DataFrame
        Job descriptions. 

    Returns
    -------
    DataFrame
        Dataframe with job, description and fact

    '''
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
    time_rdd = rdd.map(lambda x: Row(dayD= x[0], monthD=x[1], 
                                     quarterD=x[2], yearD=x[3]))
    time_df = spark.createDataFrame(time_rdd) \
                    .withColumn("idTime", monotonically_increasing_id())
    return time_df


def read_company(df): 
    '''
    Read company and location from df

    Parameters
    ----------
    df : DataFrame
        Job descriptions 

    Returns
    -------
    DataFrame with company name and working location 

    '''
    
    company = df.select('company_name', 'working_location') \
                    .distinct().count()
    return company

# def read_market_fact(df):  
    
    
if __name__ == '__main__': 
    df = read_file('../data/job_description.csv')
    read_time(df).show()