import psycopg2
import getpass
###############################################################
# Class idfdfdfdfeff                                                             #
#                                                             #
#                                                             #
###############################################################
class Inbound:
    def __init__(self):
        print("WELCOME TO POLICY ENROLL PAGE")
        # print("Welcome to hps")
        # print("Login Here")
        # user=input("User ID : ")
        # # input("Password : ")
        # passwd=getpass.getpass("Pass Key : ")
        # input("Level of Access : ")
        # get demographic information
        self.fname     = str(input("fname  "))             
        self.lname     = str(input("lname  "))          
        self.gender    = str(input("gender "))           
        self.dob       = str(input("dob    "))        
        self.email     = str(input("email  "))          
        self.addr1     = str(input("addr1  "))          
        self.addr2     = str(input("addr2  "))          
        self.pincode   = str(input("pincode"))            
        self.city      = str(input("city   "))         
        self.country   = str(input("country"))            
        self.state     = str(input("state  "))
        self.phone     = str(input("phone  ")) 
        # get policy details
        self.policy    = str(input("policy "))            
        self.ptype     = str(input("ptype  "))           
        self.incpdt    = str(input("incpdt "))            
        self.enddt     = str(input("enddt  "))           
        self.effdt     = str(input("effdt  "))           
        self.trmdt     = str(input("trmdt  "))
        # tracking module
        self.addln     =  "inbound"      
      

    def load_to_db(self):
        # Connecting to Database
        con=psycopg2.connect("dbname='hps' user='postgres' password='Lokilove@123' host='localhost' port='5432'")
        # Open Cursor
        cur=con.cursor()
        # setup data
        # sql="""
        #     insert into 
        #     subscriber
        #     (fname  ,lname  ,gender ,dob    ,email  ,addr1  ,addr2  ,pincode,city   ,country,state  ,phone  ,policy ,ptype  ,incpdt ,enddt  ,effdt  ,trmdt  ,addln  )
        #     values
        #     (%s) returning cim;
        #     """

        data=(self.fname  ,self.lname  ,self.gender ,self.dob    ,self.email  ,self.addr1  ,self.addr2  ,self.pincode,self.city   ,
        self.country,self.state  ,self.phone  ,self. policy,self.ptype  ,self.incpdt, self.enddt  ,self.effdt  ,self.trmdt  ,self.addln )
        data=('bot','ssdfasdf','f','01/01/2020','dsfsd','dsfs','sdf','sd','dsf','sdf','sdf','963','sdfds','s',
			 '01/01/2020','01/01/2020','01/01/2020','01/01/2020','dummy')
        
        sql="""insert into 
            subscriber
            (fname  ,lname  ,gender ,dob    ,email  ,addr1  ,addr2  ,pincode,city   ,country,state  ,phone  ,policy ,ptype  ,incpdt ,enddt  ,effdt  ,trmdt  ,addln  )
            values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
			returning cim;
            """
        
        # execute
        cur.execute(sql,data)
        retid = cur.fetchone()[0]
        # execute
        print(retid)
        print("Successfully Registered")

        con.commit()
# Main
