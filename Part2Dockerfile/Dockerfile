FROM python
RUN pip install urllib3 requests pandas tinys3
WORKDIR /src
RUN git clone https://github.com/vasantivmahajan/DataWrangling_AnomalyDetection_Tableau_Visualization_EDGAR_Log_Data.git
CMD ["python", "/src/Part2_EDGAR_LogDataset.py"]

