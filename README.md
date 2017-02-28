# Data_Wrangling_Edgar_Datasets
Assignment for Advances in Datasciences 


Steps to run Part-1
Approach one:-
1. Download the docker file from the repository.

2. Start docker, build the docker file.
  ```python docker build -t Team1-Part-1 . ```

DockerFile downloads python image and installs required packages. It then clones part-1 repo copies required files to container and sets command to run part-1 python script.

3. After building docker file. Run following command
```docker run -ti Team1-Part-1 ```

4. Enter CIK and Accession Number via console input. 
5. The Access Code and Secret Key are part of config.ini file.You can  edit it on github and build docker file and run container.

Approach 2:

1. Download docker image from docker hub- 
```docker pull nehalbhanushali/Data_Wrangling_Edgar_Datasets```


LUIGI WORKFLOW
Approach 3:

1. Download docker image from docker hub- 
```docker pull nehalbhanushali/Data_Wrangling_Edgar_Datasets```

2. Run container's bash
```docker run -ti nehalbhanushali/Data_Wrangling_Edgar_Datasets bash```

3. Change directory 
```cd /src```

4. Running the pipline using luigi : The luigi workflow has two task one for scrapping and creating zip folder of CSV files and second task to upload to S3.
```python3 run_luigi.py UploadToS3  --local-schedule --CIK '0000051143' --DAN '000005114313000007' --S3bucketName 'edgardatasets' ```

KNOW ISSUE
the S3 luigi has a known  issue which appears sometimes when running the job.
https://github.com/spotify/luigi/issues/1552

