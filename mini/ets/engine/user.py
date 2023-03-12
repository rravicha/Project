import faker
import psycopg2
import psycopg2.extras as pe
conn=psycopg2.connect(host="localhost",port=5432, database="ets", user="postgres", password="susi")
conn.autocommit = True
cur=conn.cursor()
f=faker.Faker()

def populate_user_table():
    sql=f'''
    INSERT INTO dev."user"(name, email, dob, address, country, state, city, account_id)
    VALUES(%s, %s, %s, %s, %s, %s,%s, %s)
    '''
    Values=(f.name(), f.email(), str(f.date_of_birth()), f.address(), f.country(), f.state(), f.city(), f.random_number())
    try:
        cur.execute(sql,Values)
    except Exception as err:
        print(f"general exception:{err}")
        


populate_user_table()



conn.close