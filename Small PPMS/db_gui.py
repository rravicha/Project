import tkinter as tk
from tkinter import *
import psycopg2

con=psycopg2.connect("dbname='bigdata' user='postgres' password='Lokilove@123' host='localhost' port='5432'")
cur=con.cursor()
win=Tk()
win.title("Cusotmer Form");win.geometry("350x350");win.resizable(False,False);

frm1=Frame(win)
frm1.pack(side=tk.LEFT,padx=20)

var1=StringVar()
var2=StringVar()
var3=StringVar()
var4=StringVar()
var5=StringVar()
var6=StringVar()
cname=StringVar()
email=StringVar()
age=StringVar()
cid=StringVar()

lbl1=Label(frm1,textvariable=var1);var1.set("Customer Name")     # Label                  
lbl2=Label(frm1,textvariable=var2);var2.set("Customer Age")      # Label            
lbl3=Label(frm1,textvariable=var3);var3.set("Email ID")          # Label        
lbl4=Label(frm1,textvariable=var4);var4.set("Enquiry")           # Label       
lbl5=Label(frm1,textvariable=var5);var5.set("Customer ID") # Label                 
lbl6=Label(frm1,textvariable=var6);var6.set("CIM")
txt1=Entry(frm1,textvariable=cname) # Entry Fields   
txt2=Entry(frm1,textvariable=age)   # Entry Fields 
txt3=Entry(frm1,textvariable=email) # Entry Fields   
txt4=Entry(frm1,textvariable=cid)   # search text




lbl1.grid(row=0,column=0);txt1.grid(row=0,column=1)
lbl2.grid(row=1,column=0);txt2.grid(row=1,column=1)
lbl3.grid(row=2,column=0);txt3.grid(row=2,column=1)

lbl4.grid(row=4,column=0)
lbl5.grid(row=5,column=0);txt4.grid(row=5,column=1)
lbl6.grid(row=3,column=2)

def addcustomer():
    cname=txt1.get();age=txt2.get();email=txt3.get()
    sql="insert into customers(name,age,email) values(%s,%s,%s) returning cust_id"
    cur.execute(sql,(cname,age,email))
    cid=cur.fetchone()[0]
    var6.set(str(cid))
    print(f"Customer Added {cid} ")
    con.commit()
    print("commit transaction")
def search():
    cim=cid.get("1.0",END)
    print(f"searching for {cim}")
    
btn1=Button(frm1,text = "add",    command=addcustomer)
btn1.grid(row=3,column=1)
btn2=Button(frm1,text = "search", command=search)
btn2.grid(row=6,column=1)








win.mainloop()