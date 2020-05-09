import wikipedia
import pandas as pd
from sqlalchemy import create_engine
res=wikipedia.search(input("Seach:"))

engine   = create_engine("postgres://scott:tiger@localhost/bigdata")

# print(res)
# page=wikipedia.page(res[0])
# print(page.content)

for i,r in enumerate(res):
    print(i+1,'  ',r)

choice=int(input("Please choose any option to brief : "))

# print(wikipedia.page(res[res(int(choice))].content))

page=wikipedia.page(res[choice])

print(page.content)

dict_df={}
dict_df['title']=page.title
dict_df[res[choice]]=page.content
df=pd.DataFrame(dict_df)
df.to_sql('wikipedia',engine,if_exists = 'append')   # writes to postgres db
