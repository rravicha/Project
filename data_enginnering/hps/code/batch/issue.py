import collections
import psycopg2

###############################################################
# Class issue:
# Validate cim and issue the policy
###############################################################
class Issue:
    def __init__(self):
        print("Issue the subscribed cim")
        self.cim=str(input("Enter the cim to be issued"))
    def issue(self):
        print(f'Cim currently in queue {self.cim}')
        con=psycopg2.connect("dbname='hps' user='postgres' password='Lokilove@123' host='localhost' port='5432'")
        cur=con.cursor()
        sql="select * from subscriber where cim= %s"
        data=(self.cim,)
        cur.execute(sql,data)
        res=cur.fetchall()
        print(res)
        
        
        