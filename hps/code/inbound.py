import getpass
import psycopg2


class Inbound:
    def __init__(self):
        print("Welcome to hps")
        print("Login Here")
        user=input("User ID : ")
        # input("Password : ")
        passwd=getpass.getpass("Pass Key : ")
        input("Level of Access : ")

    def terminal(self):
        input("WELCOME TO POLICY ENROLL PAGE")
        # get demographic information
        self.fname     = input("fname  ")              
        self.lname     = input("lname  ")           
        self.gender    = input("gender ")            
        self.dob       = input("dob    ")         
        self.email     = input("email  ")           
        self.addr1     = input("addr1  ")           
        self.addr2     = input("addr2  ")           
        self.pincode   = input("pincode")             
        self.city      = input("city   ")          
        self.country   = input("country")             
        self.state     = input("state  ") 
        self.phone     = input("phone  ")  
        # get policy details
        self.policy    = input("policy ")            
        self.ptype     = input("ptype  ")           
        self.incpdt    = input("incpdt ")            
        self.enddt     = input("enddt  ")           
        self.effdt     = input("effdt  ")           
        self.trmdt     = input("trmdt  ")
        # tracking module
        self.addln     =  "inbound"      
      

    def load_to_db(self):
        con=psycopg2.connect("dbname='hps' user='postgres' password='Lokilove@123' host='localhost' port='5432'")
        cur=con.cursor()
        cur.execute(
            '''
            insert into subscriber()
            '''
        )

	




        

# Main
