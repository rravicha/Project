import psycopg2
conn=psycopg2.connect(
  host="localhost",
    database="ets",
    user="postgres",
    password="susi")

db=conn.cursor()
conn.autocommit = True

db.execute('''select * from user;''')

for i in db.fetchall():
    print(i)