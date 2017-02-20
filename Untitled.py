
# coding: utf-8

# In[ ]:





mainUrl = "http://www.sec.gov/Archives/edgar/data/"

CIK = input("Enter CIK (eg :0000051143)")
DAN = input("Enter document accession number -DAN (eg :000005114313000007 )")


CIK = CIK.strip().strip("0")
DAN = DAN.strip()
partCIK = DAN[0:10]+"-"
partDAN = DAN[10:12]+"-"
lastPart = DAN[12:18]+"-index.html"

completeURL = mainUrl+CIK+"/"+DAN+"/"+partCIK+partDAN+lastPart
#print(completeURL)

