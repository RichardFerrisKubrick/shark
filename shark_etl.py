#!/usr/bin/env python
# coding: utf-8

# In[37]:


import csv
from datetime import datetime, timedelta
import pyodbc


# In[38]:


conn = pyodbc.connect('DSN=kubricksql;UID=DE14;PWD=password')
cur = conn.cursor()


# In[39]:


sharkfile = r'c:\data\GSAF5.csv'


# In[40]:


# always use "with" syntax as the file is automatically closed (even if system crashes). this frees up memory
# here using csv library to import csv as a dictionary
attack_date = []
case_number = []
country = []
activity = []
age = []
gender = []
isfatal = []
with open(sharkfile) as f:
    reader = csv.DictReader(f)
    for row in reader:
        attack_date.append(row['Date'])
        case_number.append(row['Case Number'])
        country.append(row['Country'])
        activity.append(row['Activity'])
        age.append(row['Age'])
        gender.append(row['Sex '])
        isfatal.append(row['Fatal (Y/N)'])


# In[41]:


data = zip(attack_date, case_number, country, activity, age, gender, isfatal)


# In[43]:


cur.execute('TRUNCATE TABLE richard.shark')


# In[44]:


q = 'INSERT INTO richard.shark (attack_date, case_number, country, activity, age, gender, isfatal) VALUES (?, ?, ?, ?, ?, ?, ?)'


# In[ ]:


for d in data:
    try:
        cur.execute(q, d)
        conn.commit()
    except:
        conn.rollback()


# In[14]:










