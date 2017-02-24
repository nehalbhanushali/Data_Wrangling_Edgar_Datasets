
# coding: utf-8

# In[10]:

#!/usr/bin/env python
def generate_url(year):
    
    #generate the url for fetching the log files for every month's first day
    number_of_months=1
    while number_of_months < 13:
        if(number_of_months <10):
            url="http://www.sec.gov/dera/data/PublicEDGAR-log-file-data/"+year+"/Qtr1/log"+year+'%02d' % number_of_months+"01.zip"
        else:
            url="http://www.sec.gov/dera/data/PublicEDGAR-log-file-data/"+year+"/Qtr1/log"+year+str(number_of_months)+"01.zip"
        number_of_months=number_of_months+1
    #temp_url=download_data("http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/2016/Qtr1/log20160101.zip")
    maybe_download("http://www.sec.gov/dera/data/Public-EDGAR-log-file-data/2016/Qtr1/log20160101.zip")
    
def maybe_download(url):
    
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
  
    
def trial_download(url):
    import zipfile
    from zipfile import ZipFile
    import urllib.request
    import csv
    import pandas as pd
    url = urllib.request.urlopen(url)
    with ZipFile(BytesIO(url.read())) as my_zip_file:
        pd.read
        for contained_file in my_zip_file.namelist():
            print("unzipping maybe")
    
def download_data(temp_url):
    print("In download URL function")
    import pandas as pd
    import zipfile
    zf = zipfile.ZipFile(temp_url)
    zf_file=zf.open('log20030101.zip')
    df = pd.read_csv(zf_file)
    #df = pd.read_csv(url, compression='zip', header=0, sep=',', quotechar='"')
    print(df.head(5))
    #you need to install request package using $ pip install requests
    """from io import BytesIO
    from zipfile import ZipFile
    import urllib.request
    import csv
    url = urllib.request.urlopen(url)
       
    with ZipFile(BytesIO(url.read())) as my_zip_file:
        for contained_file in my_zip_file.namelist():
            print("unzipping maybe")
            uncompress_size = sum((file.file_size for file in my_zip_file.infolist()))

            extracted_size = 0

            for file in my_zip_file.infolist():
                extracted_size += file.file_size
                print (extracted_size * 100/uncompress_size)
                my_zip_file.extract(file)
                
            # with open(("unzipped_and_read_" + contained_file + ".file"), "wb") as output:
            
            for line in my_zip_file.open(contained_file).readlines():
                print("reading line one by one started")
                line=[line]
                with open('test.csv', 'w', newline='') as fp:
                    a = csv.writer(fp, delimiter=',')
                    a.writerows(line)
            print("Done")"""

    
if __name__ == '__main__':
    
    #fetch the year for which the user wants logs
    year = input('Enter the year for which you need to fetch the log files: ')
    #calling the function to generate dynamic URL
    generate_url(year)


# In[ ]:




# In[ ]:



