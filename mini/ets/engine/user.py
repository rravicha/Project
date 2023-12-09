from faker import Faker
import psycopg2
import psycopg2.extras as pe

class User:
        
    def __init__(self):
        self.conn=psycopg2.connect(host="localhost",port=5432, database="ets", user="postgres", password="susi")
        self.conn.autocommit = True
        self.f=Faker()
        self.cur=self.conn.cursor()
    def auto_generate(self):
        self.name = str(self.f.name())
        self.email = str(self.f.email())
        self.date_of_birth = str(self.f.date_of_birth())
        self.address = str(self.f.address())
        self.country = str(self.f.country())
        self.state = str(self.f.state())
        self.city  = str(self.f.city())
        self.account_id = str(self.f.random_number())
    def insert(self):
        sql=f'''
            INSERT INTO dev.user(name, email, dob, address, country, state, city, account_id)
            VALUES('{self.name}','{self.email}','{self.date_of_birth}','{self.address}','{self.country}','{self.state}','{self.city}','{self.account_id}')'''
        self.cur.execute(sql)
    def parent_insert(self):
        self.cur.execute(f"insert into dev.region(country,city, state) values ('{self.country}','{self.city}','{self.state}')")
    def __exit__(self):
        self.conn.close()

if __name__=='__main__' :
    
    u=User()
    u.auto_generate()
    try: 
        u.insert()
    except psycopg2.errors.ForeignKeyViolation as error:
        el=(str(error).split(' '))
        if 'violates' in el:
            el=''.join(el[13:])
            el=el.split('isnotpresentintable')[0].split('=')
            
            print(el)
            u.parent_insert()
            u.insert()


