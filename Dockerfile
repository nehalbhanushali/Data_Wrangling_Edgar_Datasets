FROM python
RUN pip install beautifulsoup4 requests pandas
COPY . /src
CMD ["python", "/src/Scraping_with_dataframes.py"]