#!/usr/bin/env python
# coding: utf-8

# #Silver Layer - This layer is for Data Munging, Enrichment, customization and curation.  We can store the data as per the Business needs

# In[22]:


#Run the below notebook for invoking the function to this notebook
get_ipython().run_line_magic('run', '"/notebook/PysparkCode/Generic_function.ipynb"')


# ##Data Munging

# In[23]:


#Reads the file in orc format and do count level validation
df=Active_Munging("/dataset/Bronze",Struct_schema,"Corrupt_rows","Job ID")
print("Total Number of records",df.count())
print("Total Number of De-Duplicated records",df.distinct().count())
df.show()


# ##Data Enrichment - Pre processing the data

# In[24]:


#df.show()
silver_df=data_enrichment(df,["Posting Date","Post Until","Posting Updated","Process Date"],"Job ID")
silver_df.printSchema()


# ##Data Curation - transformation, filtering<br>
# List of KPIs to be resolved:<br>
# Whats the number of jobs posting per category (Top 10)?<br>
# Whats the salary distribution per job category?<br>
# Is there any correlation between the higher degree and the salary?<br>
# Whats the job posting having the highest salary per agency?<br>
# Whats the job positings average salary per agency for the last 2 years?<br>
# What are the highest paid skills in the US market?<br>

# In[25]:


#Whats the number of jobs posting per category (Top 10)?
def No_of_jobs(df):
    df1=df.groupBy(upper(col("Job Category")).alias("Job Category")).count().orderBy("Count",ascending=False).limit(10).show()
    return df

print(No_of_jobs(silver_df))


# In[26]:


#Whats the salary distribution per job category?
def Salary_distribution(df):
    SF_LIST=df.select(upper(col("Job Category")).alias("Job Category"),upper(col("Salary Frequency")).alias("Salary Frequency")).distinct()
    return SF_LIST

print(Salary_distribution(silver_df).show(truncate=False))


# ##Data Customization

# In[28]:


#Is there any correlation between the higher degree and the salary?
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re

# Define UDF to extract education level from text
def extract_education_level(qual):
    if qual is None:
        return 0
    
    text = qual.lower()
    
# Define education level patterns
    if any(term in text for term in ['phd', 'doctorate', 'doctoral']):
        return 4
    elif any(term in text for term in ['master', 'mba', 'ma degree', 'ms degree', 'graduate degree']):
        return 3
    elif any(term in text for term in ['bachelor', 'baccalaureate', 'ba degree', 'bs degree', 'undergraduate']):
        return 2
    elif any(term in text for term in ['high school', 'diploma', 'ged', 'h.s.']):
        return 1
    else:
        return 0
    
# Calculate Average Salary and annual_salary

df_with_AvgSal = silver_df.withColumn("Average_Salary",((col("Salary Range From") + col("Salary Range To")) / 2))                    .withColumn("Annual_salary",when(col("Salary Frequency") == "Annual", col("Average_Salary"))                                 .when(col("Salary Frequency") == "Hourly", col("Average_Salary") * 2080)                                 .when(col("Salary Frequency") == "Daily", col("Average_Salary") * 260)                                 .otherwise(None))


# Register UDF
extract_education_udf = udf(extract_education_level)

for col_name in silver_df.columns:
    if ' ' in col_name:
        new_col_name = col_name.replace(' ', '_')
        silver_df = silver_df.withColumnRenamed(col_name, new_col_name)

#write to table

silver_df.write.mode("overwrite").saveAsTable("NYC_Job_Enrich")

df_Enhanced=df_with_AvgSal.withColumn("Education_Level",extract_education_udf(col("Minimum Qual Requirements")))
writeFile(df_with_AvgSal,"/dataset/Silver","orc") # we can save it as table for Business review
#df_Enhanced.select("Education_Level","Annual_salary").show()
correlation = df_Enhanced.select(corr("Education_Level","Annual_salary"))
correlation.show()
#df_Enhanced.summary().show()


# In[ ]:




