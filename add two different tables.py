#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('hello').getOrCreate()


# In[10]:


data1=[('john','developer',23000),('sharma','HR',27000),('jessi','data',30000)]


# In[11]:


from pyspark.sql.types import IntegerType,StringType,StructType,StructField

schema1=StructType([
    StructField('Name',StringType(),True),
    StructField('Department',StringType(),True),
    StructField('Salary',IntegerType(),True)
])

df1=spark.createDataFrame(data=data1,schema=schema1)


# In[13]:


df1.show()


# In[37]:


contract=[('messi','developer'),('ronaldo','test')]

column=StructType([
    StructField('Name',StringType(),True),
    StructField('Department',StringType(),True)
])
df2=spark.createDataFrame(data=contract,schema=column)
df2.show()


# In[19]:


df1.printSchema()
df2.printSchema()


# In[25]:


from pyspark.sql.functions import*
df3=df2.withColumn('Salary',lit(0))


# In[24]:


df3.printSchema()


# In[34]:


df4=df1.union(df3)


# In[35]:


df4.show()


# In[ ]:




