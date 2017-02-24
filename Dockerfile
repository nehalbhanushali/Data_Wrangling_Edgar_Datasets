FROM python
RUN pip install beautifulsoup4 requests pandas tinys3
WORKDIR /src
RUN git clone  https://5fea112cb4c64457395693f982b3fd78d929a807:x-oauth-basic@github.com/nehalbhanushali/Data_Wrangling_Edgar_Datasets.git /src
CMD ["python", "/src/Scraping_with_dataframes.py"]

