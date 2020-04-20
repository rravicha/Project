import os
os.system('cls')
from bs4 import BeautifulSoup as bs
import requests
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import sys
try:
    flag=sys.argv[1]
except IndexError:
    flag=0

print(flag)
content1=[];content2=[];content3=[];content4=[];content5=[]
url = "https://www.livechennai.com/gold_silverrate.asp"
page=requests.get(url)
data=bs(page.text,'html.parser')
fnt=data.find_all('font')
fnt=iter(fnt)

next(fnt)
next(fnt)

try:
    while(True):
        # print(next(fnt).text)
        this=next(fnt)
        content1.append(this.text)
except StopIteration:
    pass
    # print(content)

with open("content.txt",'w') as fh:
    fh.writelines(content1)
with open("content.txt") as fh:
    content1=fh.readlines()

for i in range(11):
    if i==0 or i>11:
        pass
    else:
        content2.append(content1[i])

content3=[row.strip() for row in content2]
content4.append(['date','24K_1gm','24K_8gm','22K_1gm','22K_8gm'])
# content4=[list((row[:16],row[16:23],row[23:31],row[31:38],row[38:47])) for row in content3]
content4=[list((row[:13],row[13:20],row[20:28],row[28:35],row[35:])) for row in content3]
headers=('date','24K_1gm','24K_8gm','22K_1gm','22K_8gm')
# print(headers[0])
df=pd.DataFrame(content4,columns=headers)

# df.to_csv("content.csv")
print('raw df')
print(df)
print('format and replace')
df.replace('',np.NaN,inplace=True)
df.dropna(inplace=True)
print(df)
print('writing to db')

if flag==1:
    print('conneting to db')
    engine   = create_engine("postgres://scott:tiger@localhost/bigdata")
    df.to_sql('gold',engine,if_exists = 'append')   # writes to postgres db

print('pushed to db')
# q=pd.read_sql('select * from gold_tb', conn)
# print(q)
# conn.commit()
# print('commited the changes')




# conn=sqlite3.connect('gold.db')
# cur=conn.cursor()
# cur.execute(''' INSERT INTO gold_tb values ''')
print("EOP")