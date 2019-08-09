''''
Build Type : Prototype
Project Name : Supply Chain Planning
'''
#Needed imports
try:
    import pandas as pd
except ImportError as e:
    raise ImportError("package missing pandas")
else:
    print("successful import of pandas")
finally:
    print("End of pandas import")

import sys
#global variables

#Class Definition
class scp:
    def __init__(self,filename,sheet):
        self.filename=filename
        self.sheet=sheet
        
    def read_sheet(self):
        self.dmd=pd.read_excel(self.filename,sheet_name=self.sheet)
        
    def print_demand(self):
        print(self.dmd)
     
##MAIN##
if __name__ == "__main__":
    scpobj = scp(sys.argv[0],sys.argv[0])
    print("opening filename {} - sheetname {}".format(sys.argv[0],sys.argv[0]))
    scpobj.read_sheet()
    scpobj.print_demand()
