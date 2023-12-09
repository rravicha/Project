import requests
from bs4 import BeautifulSoup

get = requests.get("https://www.oneindia.com/")
html = get.text

soup = BeautifulSoup(html)

print(dir(soup))

with open('test.html','w') as fp:
    fp.writelines(soup)