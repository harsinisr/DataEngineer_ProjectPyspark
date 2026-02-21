#!/usr/bin/env python
# coding: utf-8

# #Gold Layer - For Reporting and advanced analytics
# 1. Windowing
# 2. Aggregation

# In[8]:


#Run the below notebook for invoking the function to this notebook
get_ipython().run_line_magic('run', '"/notebook/PysparkCode/Generic_function.ipynb"')


# In[9]:


df_Enhanced = spark.read.parquet("spark-warehouse/NYC_Job_Enrich")
df_Enhanced.show()


# In[ ]:


#Whats the job posting having the highest salary per agency?
from pyspark.sql.window import Window
from pyspark.sql.functions import *

def high_sal(df):
    df_highestSal=df.withColumn("Rank",rank().over(Window.partitionBy("Posting Type","Agency").orderBy(desc("Annual_salary"))))    .select("Posting Type","Agency","Annual_salary","Rank")
    return df_highestSal

high_sal(df_Enhanced).where("Rank = 1").show()


# In[ ]:


#Whats the job positings average salary per agency for the last 2 years?
from pyspark.sql.window import Window
from pyspark.sql.functions import *

def avg_sal(df):
    avg_sal=df.where("year(`Posting Date`) >= year(current_date())-2 ").groupBy("Posting Type","Agency").agg(avg(col("Annual_salary")).alias("avg_sal"))
    return avg_sal

avg_sal(df_Enhanced).show()


# In[ ]:


#What are the highest paid skills in the US market?
from pyspark.sql.window import Window

def high_paid(df):
    df1=df.groupBy("Division/Work Unit").agg(max(col("Annual_salary")).alias("highest_paid")).orderBy("highest_paid",ascending=False)
    return df1

high_paid(df_Enhanced).show(1)

