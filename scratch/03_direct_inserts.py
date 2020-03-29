#!/usr/bin/env python
# coding: utf-8

# # Add data to the database from pandas

# In[1]:


from sqlalchemy import create_engine
from sqlalchemy import Table, Column, String, MetaData, DateTime, Float, Integer
from datetime import datetime
import pandas as pd
import json
import shutil
import os
import sys


# In[2]:


postgresname = 'postgresql://postgres:mysecretpassword@localhost:5432/houselogs'


# In[3]:


db = create_engine(postgresname, echo=False)


# In[4]:


rootdir = '/home/tony/node-red-logs/backup/20200323_140620.txt'


# In[5]:


filename = rootdir
if os.path.exists(filename):
    with open(filename) as f:
        rawdata = f.read().splitlines()
else:
    rawdata = []


# In[6]:


rawdata = [json.loads(i) for i in rawdata]


# In[16]:



def get_table_def(dict_in, db_in):
    """
    Turn dictionary into a table definition for insert / update
    see: https://www.compose.com/articles/using-postgresql-through-sqlalchemy/
    """
    meta = MetaData(db_in)
    
    val_mapping = {
        'pressure': Integer,
        'temperature': Float,
        'humidity': Float,
        'battery': Integer,
        'colorTemperature': Integer,
    }
    
    val_type = val_mapping.get(dict_in['name'], String)
    

    table_def = Table(dict_in['name'], meta,  
                    Column('source', String),
                    Column('name', String),
                    Column('displayName', String),
                    Column('value', String),
                    Column('unit', String),
                    Column('deviceId', Integer),
                    Column('hubId', Integer),
                    Column('locationId', Integer),
                    Column('installedAppId', Integer),
                    Column('descriptionText', String),
                    Column('timestamp', DateTime),
                     )
    return table_def


# In[17]:


def insert_data(json_val, db):
    aa = get_table_def(samp, db)
    try:
        aa.create()
        print(f"Table {samp['name']} created!")
    except Exception as e:
        pass
    dup = aa.select().where(aa.c.value==samp['value']).where(aa.c.timestamp==samp['timestamp']).where(aa.c.deviceId==str(samp['deviceId']))
    result = db.execute(dup)
    if result.rowcount == 0:
        insert_statement = aa.insert().values(**samp)
        output = db.connect().execute(insert_statement)
        return 1
    elif result.rowcount == 1:
        return 0
    elif result.rowcount > 1:
        print(f"Duplicate data present for table {json_val['name']}")
        return 0


# In[21]:


i = 0
for samp in rawdata:
    i += insert_data(samp, db)
print(f"{i} inserted")


# In[ ]:





# In[ ]:


try:
    aa.create()
    print(f"Table {samp['name']} created!")
except Exception as e:
    print(f"Table {samp['name']} exists - not creating :(")
    pass


# In[33]:


insert_statement = aa.insert().values(**samp)


# In[34]:


output = db.connect().execute(insert_statement)


# In[35]:


output


# In[36]:


select_statement = aa.select()


# In[37]:


result_set = db.execute(select_statement)


# In[38]:


for r in result_set:
    print(r)


# In[67]:


samp


# In[16]:


aa = get_table_def(samp, db)
dup = aa.select().where(aa.c.value==samp['value']).where(aa.c.timestamp==samp['timestamp']).where(aa.c.deviceId==str(samp['deviceId']))
result = db.execute(dup)


# In[17]:


result.rowcount


# In[15]:



for r in result:
    print(r)


# In[14]:





# In[ ]:




