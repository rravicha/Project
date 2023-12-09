
import psycopg2
import time
conn=psycopg2.connect(host="localhost",port=5432, database="ets", user="postgres", password="susi")


cur=conn.cursor()
a=0
b=0
while True:
    cur.execute("select * from  dev.region  order by id desc limit 1;")
    time.sleep(1)
    a=cur.fetchone()[0]
    print(a)