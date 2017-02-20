
# coding: utf-8

# In[ ]:


from bs4 import BeautifulSoup
from collections import namedtuple
import urllib.request as ur # urllib2 is latest?
from collections import namedtuple
import pprint
import csv
import re

class Page:

    def __init__(self, url):
        """
        Retrieves and stores the urllib.urlopen object for a given url
        """

        self.link = ur.urlopen(url)
        
    def get_hyperlink(self):
        
        raw_html = self.link.read()
        soup = BeautifulSoup(raw_html, "html.parser")
        

        for link in soup.findAll('a', attrs={'href': re.compile("10q.htm$")}):
            path = link.get('href')
              
            
    def get_tables(self):
        """
        Extracts each table on the page and places it in a dictionary.
        Converts each dictionary to a Table object. Returns a list of
        pointers to the respective Table object(s).
        """

        raw_html = self.link.read()
        soup = BeautifulSoup(raw_html, "html.parser")
        tables = soup.findAll("table")

        # have to extract each entry using nested loops
        table_list = []
        for table in tables:
            # empty dictionary each time represents our table
            table_dict = {}
            rows = table.findAll("tr")
            # count will be the key for each list of values
            count = 0
            for row in rows:
                value_list = []
                entries = row.findAll("td")
                for entry in entries:
                    # fix the encoding issues with utf-8
                    entry = entry.text.encode("utf-8", "ignore")
                    #strip_unicode = re.compile("([^-_a-zA-Z0-9!@#%&=,/'\";:~`\$\^\*\(\)\+\[\]\.\{\}\|\?\<\>\\]+|[^\s]+)")
                    #entry = strip_unicode.sub(" ", entry.decode(encoding="utf-8"))
                    #print(len(entry))
                    value_list.append(entry)
                # we don't want empty data packages
                print(value_list[0])
                if len(value_list) > 0:
                    table_dict[count] = value_list
                    count += 1

            table_obj = Table(table_dict)
            #table_obj.show_table()
            table_list.append(table_obj)

        return table_list

    def save_tables(self, tables, ignore_small=False):
        """
        Takes an input a list of table objects and saves each
        table to csv format. If ignore_small is True,
        we ignore any tables with 5 entries or fewer. 
        """

        counter = 1
        for table in tables:
            if ignore_small:
                if table.get_metadata().num_entries > 5:
                    name = "table" + str(counter)
                    table.save_table(name)
                    counter += 1
            else:
                name = "table" + str(counter)
                table.save_table(name)
                counter += 1
                
                
Metadata = namedtuple("Metadata", "num_cols num_entries")

class Table:

    def __init__(self, data):
        """
        Stores a given table as a dictionary. The keys are the headings and the
        values are the data, represented as lists.
        """
        self.table_data = data

    def get_metadata(self):
        """
        Returns a Metadata object that contains the number of columns
        and the total number of entries.
        """

        col_headings = self.table_data.keys()
        num_cols = len(col_headings)
        num_entries = 0

        for heading in col_headings:
            num_entries += len(self.table_data[heading])

        return Metadata(
            num_cols = num_cols,
            num_entries = num_entries
        )

    def show_table(self):
        """
        Prints a formatted table to the command line using pprint
        """
        pprint.pprint(self.table_data, width=1)

    def save_table(self, name):
        """
        Saves a table to csv format under the given file name. 
        File name should omit the extension.
        """
        fname = name + ".csv"

        with open(fname, 'w',encoding='utf8',newline='') as outf:
            w = csv.writer(outf, dialect="excel")
            li = self.table_data.values()
            print(len(li))
            w.writerows(li)

# you can change the name that it saves the table to by calling save_table on the table object itself:
# don't include the extension in the file name
#tables[0].save_table("customName") ##### to-do

input("hskHKja")

############ Do we need main here >>??? ###

mainUrl = "http://www.sec.gov/Archives/edgar/data/"

CIK = input("Enter CIK (eg :0000051143)")
DAN = input("Enter document accession number -DAN (eg :000005114313000007 )")
name = input("What's your name? ")

CIK = CIK.strip().strip("0")
DAN = DAN.strip()
partCIK = DAN[0:10]+"-"
partDAN = DAN[10:12]+"-"
lastPart = DAN[12:18]+"-index.html"

completeURL = mainUrl+CIK+"/"+DAN+"/"+partCIK+partDAN+lastPart

html = Page(completeURL)
link1 = html.get_hyperlink()


# url that contains the tables we want
url="https://www.sec.gov/Archives/edgar/data/51143/000005114313000007/ibm13q3_10q.htm"

print(" >>>>>>> match "+completeURL == url)

page = Page(url)
tables = page.get_tables()
page.save_tables(tables, ignore_small=False)


