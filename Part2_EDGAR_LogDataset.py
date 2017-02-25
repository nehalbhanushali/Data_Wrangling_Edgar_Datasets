
# coding: utf-8

# In[3]:

#!/usr/bin/env python       

class GetData:
    
    def __init__(self):
        """
        Retrieves and stores the urllib.urlopen object for a given url
        """
        
    def generate_url(self,year):
        
        #generate the url for fetching the log files for every month's first day
        number_of_months=1
        while number_of_months < 13:
            if(number_of_months <10):
                url="http://www.sec.gov/dera/data/PublicEDGAR-log-file-data/"+year+"/Qtr1/log"+year+'%02d' % number_of_months+"01.zip"
            else:
                url="http://www.sec.gov/dera/data/PublicEDGAR-log-file-data/"+year+"/Qtr1/log"+year+str(number_of_months)+"01.zip"
            number_of_months=number_of_months+1
        #temp_url=download_data("http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/2016/Qtr1/log20160101.zip")
        return self.download_data("http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/2016/Qtr1/log20160101.zip")
        
    def download_data(self,url):

        import urllib.request
        import requests
        import os.path
        import zipfile
        import pandas as pd

        #fetching the zip file name from the URL
        file_name=url.split("/")

        #Downloading data if not already present in the cache
        if(os.path.exists("Part_2_log_datasets/"+file_name[8])):
            print("Already present")

        else:
            urllib.request.urlretrieve(url, "Part_2_log_datasets/"+file_name[8])
            print("Download complete")

        #unzip the file and fetch the csv file
        zf = zipfile.ZipFile("Part_2_log_datasets/"+file_name[8]) 
        csv_file_name=file_name[8].replace("zip", "csv")
        zf_file=zf.open(csv_file_name)

        #create a dataframe from the csv
        df = pd.read_csv(zf_file)
        return df
        
#fetch the year for which the user wants logs
year = input('Enter the year for which you need to fetch the log files: ')
#calling the function to generate dynamic URL
get_data_obj=GetData()
df=get_data_obj.generate_url(year)
        


# In[11]:

print(df.head(25))


# In[14]:

import numpy as np

#replacing empty strings with NaN 
df.replace(r'\s+', np.nan, regex=True)


# In[16]:


#replace all ip column NaN value by a default ip address 
df["ip"].fillna("255.255.255.255", inplace=True)

#replace all broser column NaN values by a string
df["browser"].fillna("Not Applicable", inplace=True)


#perform forward fill to replace NaN values by fetching the next valid value
df["date"].fillna(method='ffill')

#perform backward fill to replace NaN values by backpropagating and fetching the previous valid value
df["time"].fillna(method='bfill')


# In[17]:

df['cik'] = df['cik'].astype('int')


# In[26]:

df.insert(6, "CIK_Accession_Anamoly_Flag", "N")


# In[ ]:

import pandas as pd
import numpy as np

count=0;
for i in df['accession']:
   
    if(df['cik'][count]==i.split("-")[0].lstrip("0")):
        print("its a match")
    count=count+1
    
"""for _, value in df['accession'].iteritems():
    if(df['cik']==value.split("-")[0].lstrip("0")):
        print("Its a match")
if(np.where(df['accession'].values.split("-")[0].lstrip("0")).astype('int')==df['cik']):
    print("its a match")"""
   
    

