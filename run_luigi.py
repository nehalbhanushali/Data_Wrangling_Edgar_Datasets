# Filename: run_luigi.py
from bs4 import BeautifulSoup
from collections import namedtuple
import urllib as ur # urllib2 is latest?
from collections import namedtuple
import pprint
import csv
import re
import pandas as pd
import os
import logging
import zipfile
import sys
import tinys3
import Page
import luigi

class Part1(luigi.Task):
    CIK = luigi.Parameter()
    DAN = luigi.Parameter()

    def requires(self):

        return []
 
    def output(self):
        return []
 
    def run(self):
        mainUrl = "http://www.sec.gov/Archives/edgar/data/"

        #CIK = input("Enter CIK (eg :0000051143)")
        #DAN = input("Enter document accession number -DAN (eg :000005114313000007 )")
        # CIK=ConfigSectionMap("Part_1")['cik']
        # DAN=ConfigSectionMap("Part_1")['dan']
        self.CIK = self.CIK.strip().strip("0")
        self.DAN = self.DAN.strip()
        partCIK = self.DAN[0:10]+"-"
        partDAN = self.DAN[10:12]+"-"
        lastPart = self.DAN[12:18]+"-index.html"

        completeURL = mainUrl+self.CIK+"/"+self.DAN+"/"+partCIK+partDAN+lastPart
        print(completeURL)
        html = Page.Page(completeURL)
        link1 = html.get_hyperlink()
        print(link1)




        logger = logging.getLogger()
        page = Page.Page(link1)
        tables = page.get_tables()
        page.create_directory("EdgarFiles/"+self.CIK)
        tempCIK=self.CIK
        page.save_tables(tables,tempCIK,ignore_small=False)
        page.create_zip_folder('EdgarFiles')
        return []



class UploadToS3(luigi.Task):
    S3bucketName = luigi.Parameter()
    CIK = luigi.Parameter()
    DAN = luigi.Parameter()
    def requires(self):
        #return []
        return [Part1(CIK=self.CIK,DAN=self.DAN)]
 
    def output(self):
        return []
 
    def run(self):
        page = Page.Page('http://www.sec.gov/Archives/edgar/data/51143/000005114313000007/0000051143-13-000007-index.html')
        name=self.S3bucketName
        page.upload_zip_to_s3(name)
        return []







 

                 
if __name__ == '__main__':
    luigi.run()
