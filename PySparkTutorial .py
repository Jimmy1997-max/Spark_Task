#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os


# In[2]:


print(os.environ['SPARK_HOME'])


# In[3]:


print(os.environ['JAVA_HOME'])


# In[4]:


print(os.environ['PATH'])


# In[5]:


import findspark


# In[6]:


findspark.init()


# In[7]:


import pyspark


# In[8]:


from pyspark import SparkContext,SparkConf


# In[9]:


from pyspark.sql import SparkSession


# In[10]:


spark = SparkSession      .builder      .appName("Python Spark Sql Basic Exmaple")      .config("spark.driver.cores","1")      .config("spark.driver.memory","1g")      .config("spark.executor.cores","2")      .config("spark.executor.memory","1gb")      .config("spark.num.executors","2")      .enableHiveSupport()      .getOrCreate()

sc = spark.sparkContext


# In[12]:


spark.sparkContext.getConf().getAll()


# In[13]:


df = spark.sql("Select 'spark' as Hello")


# In[14]:


df.show()


# In[16]:


nums = sc.parallelize([1,2,3,4])
nums.map(lambda x : x*x).collect()


# In[ ]:


spark.stop()

