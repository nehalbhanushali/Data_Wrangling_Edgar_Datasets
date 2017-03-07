# Data_Wrangling_Edgar_Datasets

## Approach 1 : DockerHub image : Quick approach

### Steps to run Part-1

1.Download docker image from docker hub- 
```
docker pull nehalbhanushali/Data_Wrangling_Edgar_Datasets:part1
```

2.Run the image
```
docker run -ti nehalbhanushali/Data_Wrangling_Edgar_Datasets:part1
```

3.Enter CIK and Accesion number on prompt

4.Enter S3 access key,secret key and bucket name (Provide an existing bucket name)

### Steps to run Part-2

1.Download docker image from docker hub- 
```
docker pull nehalbhanushali/Data_Wrangling_Edgar_Datasets:part2
```

2.Run the image
```
docker run -ti nehalbhanushali/Data_Wrangling_Edgar_Datasets:part2
```

3.Enter the year for which the data is desired on prompt. The year must be between 2003 and 2016 as the data is available only for this duration.

4.Enter S3 access key,secret key and bucket name (Provide an existing bucket name). You may proceed towards the analysis if you do not wish to upload the folders to S3 by hittin Y on prompt.


## Approach 2 : Dockerfile : (for both Part-1 and Part-2)

1.Download the docker file from the repository.

2.Start docker, build the docker file.
  ```
  python docker build -t <desired-image-name> .
  ```

DockerFile downloads python image and installs required packages. It then clones part-1 repo copies required files to the container and sets command to run the part-1 python script.

3.After building docker file. Run following command
```
docker run -ti <image-name-you-just-provided>
```

4.Enter the required inputs on prompt


##  Approach 3: LUIGI WORKFLOW

1.Download the docker image from docker hub- 
```
docker pull nehalbhanushali/Data_Wrangling_Edgar_Datasets:part1
```

2.Run the container's bash
```
docker run -ti nehalbhanushali/Data_Wrangling_Edgar_Datasets:part1 bash
```

3.Change directory 
```
cd /src
```

4.Running the pipline using luigi : The luigi workflow has two task one for scrapping and creating zip folder of CSV files and second task to upload to S3.
```
python3 run_luigi.py UploadToS3  --local-schedule --CIK '0000051143' --DAN '000005114313000007' --S3bucketName '<yourbucketname>'
```

### KNOWN ISSUE
the S3 luigi has a [known issue](https://github.com/spotify/luigi/issues/1552) which appears sometimes when running the job.


