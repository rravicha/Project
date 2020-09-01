'''
PPMS			: 	224259_001
Project Title	:	Automate Non Ocr PDF formatting
Program Name 	:	pdfocr_glob.py
Execute Method 	:	Shell/Crontab
Usage			:	`python3 pdfocr_glob.py`
Input			:	Automatically takes files present in container directory
Description		:	The program is part of automation initative to process the file that falls in 
					container directory. The input file should be a Non-Ocr pdf file and the output
					will be in the form of pdf
'''
import os
from issue import Issue
from inbound import Inbound
select=""
def choice():
    print("Welcome to hps")
    print("1) Enroll Policy\n 2)Issue\n 3) Modify Demographics/Coverage 4)Add/Remove Dependent 5) Term 6)Reinstate 7)View")
    global select
    select=int(input())
def process():
    if select==1:
        inb=Inbound()
        inb.load_to_db()
    if select ==2:
        iss=Issue()
        iss.issue()
if __name__=='__main__':
    choice()
    process()