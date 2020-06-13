#!/usr/bin/python3
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
from demand import Demand
from onhand import Onhand

file=r'S:\repo\Project\vfcorp\data\sap.xlsx'




def demand_read():
	obj=Demand(file,'demand')
	print('imported')
	dmd_df=obj.read()
	print('return to base')
	print(dmd_df)
def onhand_read():
	obj=Onhand(file,'onhand')
	print('imported')
	ohd_df=obj.read()
	print('return to base')
	print(ohd_df)

#main
if __name__=="__main__":
	demand_read()
	onhand_read()