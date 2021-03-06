from bs4 import BeautifulSoup as bs
import requests
import pandas as pd
from sqlalchemy import create_engine

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
# df.to_sql(current_table,engine,if_exists = 'append')   # writes to postgres db
    
