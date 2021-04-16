# https://github.com/hknarra/TextScrappingToMongo.git
from bs4 import BeautifulSoup as bs
from urllib.request import urlopen as ureq
import pandas as pd
import pymongo


#class = wikitable plainrowheaders sortable jquery-tablesorted
wiki = 'https://en.wikipedia.org/wiki/Template:COVID-19_pandemic_data'

page=ureq(wiki)

page_parse = bs(page)

#all_tables=page_parse.find_all('table')

required_table=page_parse.find('table', {'class': 'wikitable plainrowheaders sortable'})

#Generate lists
A=[]
B=[]
C=[]
D=[]

for row in required_table.findAll("tr"):
    cells1 = row.findAll('th')
    cells2 = row.findAll('td')
    #Country_name=row.findAll('th') #To store second column data
    if len(cells2)>1: #Only extract table body not heading
        A.append(cells1[1].find(text=True))
        B.append(cells2[0].find(text=True))
        C.append(cells2[1].find(text=True))
        D.append(cells2[2].find(text=True))

E=[]
for b in B:
    E.append(b.strip('\n'))

F=[]
for c in C:
    F.append(c.strip('\n'))

G=[]
for d in D:
    G.append(d.strip('\n'))

#import pandas to convert list to data frame

df=pd.DataFrame(data=A, columns=['Country_name'])
df['Total_cases']=E
df['deaths']=F
df['recovery']=G

dbConn = pymongo.MongoClient("mongodb://localhost:27017/")
db = dbConn['hk_DB1']
collection = db['corona_data1']

df.reset_index(inplace=True)
data_dict = df.to_dict("records")   # to dict
# Insert collection
collection.insert_many(data_dict)     # into DB





