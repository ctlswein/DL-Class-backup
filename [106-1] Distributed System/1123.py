
# coding: utf-8

# In[1]:


import pyspark
from pyspark import SparkContext


# In[2]:


sc=SparkContext('local','spark')


# In[3]:


textfile = sc.textFile("/home/w/test.txt")


# In[4]:


textfile


# In[5]:


stringRDD = textfile.flatMap(lambda line:line.split(" "))


# In[6]:


stringRDD.collect()


# In[7]:


countRDD = stringRDD.map(lambda word:(word,1)).reduceByKey(lambda x,y:x+y)
countRDD.collect()


# In[8]:


countRDD.saveAsTextFile("output")


# In[9]:


initRDD=sc.parallelize([3,1,2,5,5])


# In[10]:


initRDD.collect()


# In[12]:


def addone(x):
    return x+1


# In[13]:


initRDD.map(addone).collect()


# In[14]:


initRDD.map(lambda x:x+1).collect()

