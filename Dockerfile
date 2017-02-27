FROM python
RUN pip install beautifulsoup4 requests pandas tinys3
WORKDIR /src
RUN git clone  https://89bcedda4545305eb4910f445d22976f76818767:x-oauth-basic@github.com/nehalbhanushali/Data_Wrangling_Edgar_Datasets.git /src
CMD ["python", "/src/Scraping_with_dataframes.py"]

