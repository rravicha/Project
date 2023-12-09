'''
version notes:
12-22-2019 - Initial Link to Production
04-18-2020 - Added Database configuration to load resulting dataframe to db.
04-20-2020 - Defect#1 gold_list4 list malfunction fixed.
04-22-2020 - Added logic to extract silver and load to database
'''
import os
os.system('cls')
from bs4 import BeautifulSoup as bs
import requests
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import sys
gold_list1=[];gold_list2=[];gold_list3=[];gold_list4=[];gold_list5=[]
url = "https://www.livechennai.com/gold_silverrate.asp"
page=requests.get(url)
data=bs(page.text,'html.parser')
fnt=data.find_all('font')
fnt=iter(fnt)

next(fnt)
next(fnt)
idx=0
try:
    while(True):
        # print(next(fnt).text)
        this=next(fnt)
        print(f"index {idx}")
        print(this.text)
        gold_list1.append(this.text)
        idx+=1
except StopIteration:
    pass
    print(gold_list1)

with open("gold_list.txt",'w') as fh:
    fh.writelines(gold_list1)
with open("gold_list.txt") as fh:
    gold_list1=fh.readlines()
print('gold list 1 after formatting')
print(gold_list1)

for i in range(11):
    if i==0 or i>11:
        pass
    else:
        gold_list2.append(gold_list1[i])

gold_list3=[row.strip() for row in gold_list2]
gold_list4.append(['date','24K_1gm','24K_8gm','22K_1gm','22K_8gm'])
# gold_list4=[list((row[:16],row[16:23],row[23:31],row[31:38],row[38:47])) for row in gold_list3]
gold_list4=[list((row[:13],row[13:20],row[20:28],row[28:35],row[35:])) for row in gold_list3]
headers=('date','24K_1gm','24K_8gm','22K_1gm','22K_8gm')
# print(headers[0])
df=pd.DataFrame(gold_list4,columns=headers)

# df.to_csv("gold_list.csv")
print('raw df')
print(df)
df.to_html('xx.html')
print('format and replace')
df.replace('',np.NaN,inplace=True)
df.dropna(inplace=True)
print(df)
print('writing to db')           


print('conneting to db')
engine   = create_engine("postgres+psycopg2://scott:tiger@localhost/bigdata")
# df.to_sql('gold',engine,if_exists = 'append')   # writes to postgres db

print('pushed to db')
'''
# q=pd.read_sql('select * from gold_tb', conn)
# print(q)
# conn.commit()
# print('commited the changes')
# conn=sqlite3.connect('gold.db')
# cur=conn.cursor()

'''
# print("Gold Rates loaded\n")

# print('Extracting for silver\n')
# silver_str=gold_list1[-1]
# print(silver_str)
# silver_list=silver_str[22:318].split(' ')
# silver_dict={}
# for row in silver_list:
#     key=row[0:13];value=row[13:18]
#     silver_dict[key]=value

# print("final dict")
# print(silver_dict)
# silver_headers=('date','rate')
