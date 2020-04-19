import os
os.system('cls')
from bs4 import BeautifulSoup as bs
import requests
import pandas as pd
# import sqlite3
# import psycopg2
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
content4=[list((row[:16],row[16:23],row[23:31],row[31:38],row[38:47])) for row in content3]
headers=('date','24K_1gm','24K_8gm','22K_1gm','22K_8gm')
# print(headers[0])
df=pd.DataFrame(content4,columns=headers)

# df.to_csv("content.csv")
print(df)
print('writing to db')
# data = <This is going to be your pandas dataframe>

# conn = sqlite3.connect('gold_db')
# cur = conn.cursor()
# cur.execute('''DROP TABLE IF EXISTS SA''')
print('conneting to db')
# conn=psycopg2.connect()
engine   = create_engine("postgres://scott:tiger@localhost/bigdata")
# df.to_sql('gold_tb', conn, if_exists='replace', index=False) # - writes the pd.df to SQLIte DB
current_table = 'gold'
if flag==1:
    df.to_sql(current_table,engine,if_exists = 'append')   # writes to postgres db
    
print('pushed to db')
# q=pd.read_sql('select * from gold_tb', conn)
# print(q)
# conn.commit()
print('commited the changes')




# conn=sqlite3.connect('gold.db')
# cur=conn.cursor()
# cur.execute(''' INSERT INTO gold_tb values ''')
print("EOP")