#!/usr/bin/python3
'''
Sub Module : Called from main.py
'''
import pandas as pd
class Demand:
	def __init__(self,file,tab):
		self.file=file
		self.tab=tab
		print('hit init')
	
	def read(self):
		demand_df=pd.read_excel(self.file,self.tab)
		# print('hit read')
		return(demand_df)
	

