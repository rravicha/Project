'''
Generic module to execute any query
'''
import mariadb

class mariacon:
    def __init__(self,):
        self.conn = mariadb.connect(user="root",password="susi",host="localhost",port=3306,database="test")
        self.cursor=self.conn.cursor()
    def commit(self):
        self.conn.commit()
    def rollback(self):
        self.conn.rollback()
    def run_query(self,query,paramlist):
        self.query      =   query
        self.paramlist  =   paramlist
        # self.cursor.execute(self.query.format(self.paramlist))
        result          =   self.cursor.fetchall()
        return result
        
    def __repr__(self):
        return(f"conn {self.conn}  ")