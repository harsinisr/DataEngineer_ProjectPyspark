#!/usr/bin/env python
# coding: utf-8

# #Broze_Layer - This layer is for storing raw data

# In[1]:


#Run the below notebook for invoking the function to this notebook
get_ipython().run_line_magic('run', '"/notebook/PysparkCode/Generic_function.ipynb"')


# In[2]:


#read the file as is from the source
bronze_df=read_csv("/dataset/nyc-jobs.csv")
#write the file in orc format for performance optimization
writeFile(bronze_df,"/dataset/Bronze","orc")

