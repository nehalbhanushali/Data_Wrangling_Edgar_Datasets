FROM python
RUN pip install beautifulsoup4 requests pandas
WORKDIR /src
COPY . /src
CMD ["python", "/src/Scraping_with_dataframes.py"]