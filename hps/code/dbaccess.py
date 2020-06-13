import psycopg2
class connect:
    def __init__(self):
        # Connecting to Database
        param={
            'dbname'    :   'hps'           ,
            'user'      :   'postgres'      ,
            'password'  :   'Lokilove@123'  ,
            'host'      :   'localhost'     ,
            'port'      :   '5432'          #,
        }
        con=psycopg2.connect(**param)
        # Open Cursor
        cur=con.cursor()