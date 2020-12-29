import pandas as pd
import sys
import os;os.system('clear')
import json
path='/home/susi/github/Project/payroll/data/DataCollectionTemplate.xlsx'
sheet_by_col={}
# sys.path.append(path)

sheets=pd.ExcelFile(path).sheet_names
# print(sn)
for sheet in sheets:
    cols=pd.read_excel(path,sheet_name=sheet,header=3)
    cols = list(cols)[1:]
    sheet_by_col[sheet] = cols
    # input("hold")
# print(str(sheet_by_col))
# df=pd.read_excel(path,sheet_name='PDP1')
db=json.dumps(list(sheet_by_col))
db=json.dumps(sheet_by_col)
print(db)