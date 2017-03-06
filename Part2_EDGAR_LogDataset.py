
# coding: utf-8

# In[1]:

import requests
import pandas as pd
import numpy as np
#import urllib3 as ur
import urllib.request as ur
import configparser
import os.path
import zipfile
import tinys3
import sys
import logging

import logging as log



# In[2]:

Config = configparser.ConfigParser()
Config.read('config.ini')
Config.sections()
def ConfigSectionMap(section):
    dict1 = {}
    options = Config.options(section)
    for option in options:
        try:
            dict1[option] = Config.get(section, option)
            if dict1[option] == -1:
                DebugPrint("skip: %s" % option)
        except:
            print("exception on %s!" % option)
            dict1[option] = None
    return dict1


# In[5]:

#!/usr/bin/env python       
merged_dataframe=pd.DataFrame()
df_list_global=list()
class GetData:
   
    def __init__(self):
        """
        Retrieves and stores the urllib.urlopen object for a given url
        """
    def create_directory(self,path):
        try:
            if not os.path.exists(path):
                os.makedirs(path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise
    
    def setDataFrame(self, df):
        merged_dataframe = df
        
    def getDataFrame(self):
        return merged_dataframe
    
    def setDataFrameList(self, list_of_df):
        
        df_list_global = list_of_df
      
        
    def getDataFrameList(self):
        return df_list_global
    
    def maybe_download(self, url_list, year):
        
        df_list=['df1','df2','df3','df4','df5','df6','df7','df8','df9','df10','df11','df12']
        year=str(year)
        count=0
        print("Downloading data for all the months")
        log.info("Downloading data for all the months")
        
        for i in url_list:

            #fetching the zip file name from the URL
            file_name=i.split("/")
           

            #Downloading data if not already present in the cache
            if(os.path.exists("Part_2_log_datasets_trial/"+year+"/"+file_name[8])):
                print("Data for ",file_name[8]," is already present, pulling it from cache")
                

            else:
                #pbar = ProgressBar(widgets=[Percentage(), Bar()])
                ur.urlretrieve(i, "Part_2_log_datasets_trial/"+year+"/"+file_name[8])
                #ur.urlretrieve(i, "Part_2_log_datasets_trial/"+year+"/"+file_name[8], reporthook)
                print("Data for ",file_name[8],"not present in cache. Downloading data")
                
            
            #unzip the file and fetch the csv file
            zf = zipfile.ZipFile("Part_2_log_datasets_trial/"+year+"/"+file_name[8]) 
            csv_file_name=file_name[8].replace("zip", "csv")
            zf_file=zf.open(csv_file_name)
            
            
            #create a dataframe from the csv and append it to the list of dataframe
            df_list[count]=pd.read_csv(zf_file)
           
            count=count+1 
        
        print("All the files are downloaded and unzipped")
        log.info("All the files are downloaded and unzipped")
        print("Creating a dataframe from the csv and appending it to the list of dataframe")
        log.info("Creating a dataframe from the csv and appending it to the list of dataframe")
        
        self.setDataFrameList(df_list)
        log.info("Merging the dataframe")
        #merging the data into one dataframe
        merged_dataframe=pd.concat([df_list[0],df_list[1],df_list[2],df_list[3],df_list[4],df_list[5],df_list[6],df_list[7],df_list[8],df_list[9],df_list[10],df_list[11]], ignore_index=True)
        self.setDataFrame(merged_dataframe)
        return merged_dataframe
    

    def generate_url(self, year):
        log.info('In generate URL method')
        print("Generating the URL's for the year")
        log.info("Generating the URL's for the year")
        url_list=list()
        #generate the url for fetching the log files for every month's first day
        number_of_months=1

        while number_of_months < 13:
            #find the quarter for the month
            if number_of_months >= 1 and number_of_months < 4:
                quarter="Qtr1"
            elif(number_of_months >= 4 and number_of_months < 7):
                quarter="Qtr2"
            elif(number_of_months >= 7 and number_of_months < 10):
                quarter="Qtr3"
            elif(number_of_months >= 10 and number_of_months < 13):
                quarter="Qtr4"

            if(number_of_months <10):
                url="http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/"+str(year)+"/"+quarter+"/log"+str(year)+'%02d' % number_of_months+"01.zip"

            else:
                url="http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/"+str(year)+"/"+quarter+"/log"+str(year)+str(number_of_months)+"01.zip"
            
            
            url_list.append(url)
            number_of_months=number_of_months+1

        return self.maybe_download(url_list,year)
        
    def fetch_year(self):
        year = input('Enter the year for which you need to fetch the log files: ')
        self.create_directory("Part_2_log_datasets_trial/"+year+"/")
        log.basicConfig(filename='Part_2_log_datasets_trial/EDGAR_LogFileDataset_LogFile.log', level=logging.INFO, format='%(asctime)s %(message)s')

        
        #fetch the year for which the user wants logs
       
        log.info('Start of program')
        year=int(year)
        if(year >= 2003 and year < 2016):
            #calling the function to generate dynamic URL
            return self.generate_url(year)
        else:
            print("EDGAR log files are available for years 2003-2016. Kindly enter a year within this range")
            fetch_year()
    
                
    def create_zip_folder(self,path):
        zipfolder_name=path+'.zip'
        zf = zipfile.ZipFile(zipfolder_name, "w")
        for dirname, subdirs, files in os.walk(path):
            zf.write(dirname)
            for filename in files:
                zf.write(os.path.join(dirname, filename))
        zf.close()
    """
    def upload_zip_to_s3(self,path):
        S3_ACCESS_KEY = ConfigSectionMap("Part_1")['s3_access_key']#'AKIAICSMTFLAR54DYMQQ'
        S3_SECRET_KEY = ConfigSectionMap("Part_1")['s3_secret_key']#'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'
        BUCKET_NAME = ConfigSectionMap("Part_1")['s3_bucket']
        #host = ConfigSectionMap("Part_1")['HOST']
        # host='edgardatasets.s3-website-us-west-2.amazonaws.com'
        # Creating a simple connection
        conn = tinys3.Connection(S3_ACCESS_KEY,S3_SECRET_KEY)

        # Uploading a single file
        f = open("Part_2_log_datasets_trial.zip",'rb')
        conn.upload("Part_2_log_datasets_trial.zip",f,BUCKET_NAME) 
    """     
        
        
    def upload_zip_to_s3(self,filetoupload):
        S3_ACCESS_KEY= input("Enter S3_ACCESS_KEY : ")
        S3_SECRET_KEY =  input("Enter S3_SECRET_KEY : ")
        
#         S3_ACCESS_KEY = ConfigSectionMap("Part_1")['s3_access_key']#'AKIAICSMTFLAR54DYMQQ'
#         S3_SECRET_KEY = ConfigSectionMap("Part_1")['s3_secret_key']#'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'
#         try:
        conn = tinys3.Connection(S3_ACCESS_KEY,S3_SECRET_KEY)
        #bucket = conn.create_bucket('edgar-data-set')
        bucket = input("Enter BUCKET_NAME : ")
            # Uploading a single file
        f = open(filetoupload,'rb')
        print("this is f",f)
        print("this is file to upload",filetoupload)
        print("this is bucket",bucket)
        conn.upload(filetoupload,f,bucket)
        print("Upload to s3 successfull")
           
#         except Exception:
#             print("INVALID keys, please try again")
#             self.upload_zip_to_s3(filetoupload)
        #host='edgardatasets.s3-website-us-west-2.amazonaws.com'
        # Creating a simple connection      
        
get_data_obj=GetData()
merged_dataframe=get_data_obj.fetch_year()
#fetch the year for which the user wants logs
#year = input('Enter the year for which you need to fetch the log files: ')
#calling the function to generate dynamic URL

#df=get_data_obj.generate_url(year)


class Process_and_analyse_data():
    
    def __init__(self):
        """
        Retrieves and stores the urllib.urlopen object for a given url
        """
    
    def format_dataframe_columns(self):
        #convert all the integer column in int format
        log.info("Data fetched, started cleaning")
        print("Data fetched, started cleaning")
        df['zone'] = df['zone'].astype('int')
        df['cik'] = df['cik'].astype('int')
        df['code'] = df['code'].astype('int')
        df['idx']=df['idx'].astype('int')
        df['norefer']=df['norefer'].astype('int')
        df['noagent']=df['noagent'].astype('int')
        df['find']=df['find'].astype('int')
        df['crawler']=df['crawler'].astype('int')
        
        #replacing empty strings with NaN 
        df.replace(r'\s+', np.nan, regex=True)
        log.info("Formatted the columns of the dataframe")
        print("Formatted the columns of the dataframe")
        self.handle_nan_values()
        
        
        
    def handle_nan_values(self):
        
        #replace all ip column NaN value by a default ip address 
        df["ip"].fillna("255.255.255.255", inplace=True)

        #perform forward fill to replace NaN values by fetching the next valid value
        df["date"].fillna(method='ffill')

        #perform backward fill to replace NaN values by backpropagating and fetching the previous valid value
        df["time"].fillna(method='bfill')

        #replace all zone column NaN values by 'Not Available' extension
        df["zone"].fillna("Not Available", inplace=True)

        #replace all extension column NaN values by default extension
        df["extention"].fillna("-index.htm", inplace=True)

        #replace all size column NaN values by 0 and convert the column into integer 
        df["size"].fillna(0, inplace=True)
        df['size'] = df['size'].astype('int')

        #replace all user agent column NaN values by the default value 1 (no user agent)
        df["noagent"].fillna("Not Applicable", inplace=True)

        #replace all find column NaN values by the default value 0 (no character strings found)
        df["find"].fillna(0, inplace=True)

        #replace all broser column NaN values by a string
        df["browser"].fillna("Not Available", inplace=True)
        
        
        
        # if the value in idx column is missing, check the value of the extension column, if its "-index.html" set the column's value 1 else 0
        count=0
        for i in df['idx']:
            if(np.isnan(i)):
                if(df['extension'][count]=="-index.htm"):
                    i=1
                else:
                    i=0
            count=count+1

        # if the value of norefer column is missing, check the value of the find column, if it is 0, set the value 1, else it set it 0
        counter=0
        for i in df['norefer']:
            if(np.isnan(i)):
                if(df["find"][counter]==0):
                    i=1
                else:
                    i=0
            counter=counter+1

        # if the value of crawler is missing, check the value of the code, if it is 404 set it as 1 else 0
        count_position=0
        for i in df['crawler']:
            if(np.isnan(i)):
                if(df["code"][count_position]==404):
                    i=1
                else:
                    i=0
            count_position=count_position+1
        log.info("Replacing NaN values with appropriate replacement")
        log.info("Handling missing values completed")
        print("Handling missing values completed")
        
        log.info("Exporting merged dataframe to local system")
        print("Exporting merged dataframe to local system")
        df.to_csv("Part_2_log_datasets_trial/merged_dataframe.csv")
        log.info("Merged dataframe exported")
        print("Merged dataframe exported")
        
        merged_dataframe
        self.identify_cik_accession_number_anomaly()
    
    def fetch_company_name_from_cik(self):
        #we found a list of CIK their company names from a EDGAR's github repository. We are fetching this information to gain the information about company name
        company_df = pd.read_csv('CIK-mapping.csv') 
        #renaming column, so that both the dataframes can be merged on the common column
        company_df = company_df.rename(columns={'CIK': 'cik'})
        company_df['cik'] = company_df['cik'].astype(np.int64)
        #merging both the dataframes
        #merged_df_cik_company_name= df.join(company_df, on='cik', how='left', rsuffix="_review")
        merged_df_cik_company_name=pd.merge(df, company_df, on='cik')
        #merged_df_cik_company_name=pd.merge(df,company_df, left_on='cik',right_on='CIK' )
        #print(merged_df_cik_company_name.head(10))
        merged_df_cik_company_name
        self.identify_cik_accession_number_anomaly()
        
        
    def identify_cik_accession_number_anomaly(self):
        #this operation requires a large amount of time for computaton, thus we are performing this on a subset of data
        small_df=df.head(25)
        #insert a column to check CIK, Accession number discripancy
        small_df.insert(6, "CIK_Accession_Anamoly_Flag", "N")
                
        #check if CIK and Accession number match. The Accession number is divided into three parts, CIK-Year-Number_of_filings_listed.
        #the first part i.e the CIK must match with the CIK column. If not, there exists an anomaly

        count=0;
        print("Creating CIK_Accession_Anomaly_Flag column to check anomaly")
        log.info("Creating CIK_Accession_Anomaly_Flag column to check anomaly")
        
        for i in small_df['accession']:
            #fetch the CIK number from the accession number and convert it into integer
            list_of_fetched_cik_from_accession=[(int(i.split("-")[0]))]

            #check if the CIK number from the column and CIK number fetched from the accession number are equal
            if(small_df['cik'][count]!=list_of_fetched_cik_from_accession):
                small_df['CIK_Accession_Anamoly_Flag'][count]="Y"

            count=count+1
        log.info("CIK Accession Anomaly flag computed")
        print("CIK Accession Anomaly flag computed")
        small_df
        self.get_file_name_from_extension()
        
    def get_file_name_from_extension(self):
        #this operation requires a large amount of time for computaton, thus we are performing this on a subset of data
        small_df=df.head(25)
        small_df.insert(7, "filename", "")
        print("Creating filename column")
        log.info("Creating filename column")
        #Extension rule: if the file name is missing and only the file extension is present, then the file name is document accession number
        count=0
        for i in small_df["extention"]:
            if(i==".txt"):
                # if the value in extension is only .txt, fetch the accession number and append accession number to .txt
                #list_of_fetched_cik_from_accession=int(((df2["accession"].str.split("-")[count])[0]))
                #print((df["accession"]).astype(str))
                #list_of_fetched_cik_from_accession=int(df["accession"])
                small_df["filename"][count]=(small_df["accession"][count])+".txt" 
            else:
                small_df["filename"][count]=i
            count=count+1
        print("Filename column created")
        small_df
        log.info("Filename column created")
        
get_data_obj=GetData()
df=get_data_obj.getDataFrame()
df_list=get_data_obj.getDataFrameList()
process_data_obj=Process_and_analyse_data()
process_data_obj.format_dataframe_columns()

log.info("Zipping the folder for loading in S3")
get_data_obj.create_zip_folder("Part_2_log_datasets_trial")
get_data_obj.upload_zip_to_s3("Part_2_log_datasets_trial.zip")
log.info("Data zipped and loaded on S3")
print("Data zipped and loaded on S3")
log.info("Pipeline completed!!")
log.info("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")


# In[ ]:

combined_df = pd.read_csv("Part_2_log_datasets_trial/merged_dataframe.csv") #  pass your 12 month combined csv here
# group by cik and date and get count of ciks for a date   
temp_df=combined_df.groupby(['cik','date'])['cik'].count()
temp_df.head()


# In[ ]:

# convert group by result into a frame

grouped_frame = pd.DataFrame(temp_df.reset_index(name = "hit_count"))

grouped_frame


# In[ ]:

## Monitor change in hit count

def get_percent_change(curr, prev):
        change_in_perc = ((curr - prev)/prev ) * 100
        return change_in_perc

count = 0
analysis_df = pd.DataFrame()
frame_count = 0
for row in grouped_frame['cik']:
    current_cik = grouped_frame['cik'][count]
    current_hit_count = grouped_frame['hit_count'][count]
    current_date = grouped_frame['date'][count]
    if(count >= 1):
        if(current_cik == grouped_frame['cik'][count-1]):
            change_in_count = current_hit_count - grouped_frame['hit_count'][count-1] 
            change_in_perc = get_percent_change(current_hit_count,grouped_frame['hit_count'][count-1])
            
            if(change_in_perc >= 1000 ): ## decide on threshold
                analysis_df.loc[frame_count, 'cik'] = current_cik
                analysis_df.loc[frame_count, 'date'] = current_date
                analysis_df.loc[frame_count, 'change in %'] = change_in_perc
                frame_count += 1
                #print(current_cik ," changed by",change_in_perc," % on ",current_date)
                
    count +=1
    
analysis_df


# In[ ]:


# Load the data into a DataFrame
data = pd.read_csv('Part_2_log_datasets_trial/merged_dataframe.csv') # pass your single month stuff here
#grouping by IP
byIp = data.groupby('ip')
byIp


# In[ ]:

byCIK = data.groupby('cik')
byCIK['size'].max()


# In[ ]:

#getting requests with  status code 404
byIp404=data[data['code']==404]
byIp404
#Anamoly-request with 404 has a download size associated.


# In[ ]:

byIp404=data[data['code']==404].groupby('ip')
byIp404['size'].mean()


# In[ ]:

#1. Simple describe function on data
summary = data.describe()
summary
#Analysis: total number of requests on this day-  261289
#Average request per day :215
#Max file download size:5.259544e+07
#Avergae File donwload size:	1.703470e+05

