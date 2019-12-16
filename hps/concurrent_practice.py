import concurrent.futures
import time

class base:
	def __init__(self,i):
		self.infile=i
		print(f'ini file {self.infile}')
	def process(self):
		with open(self.infile,'r') as fh:
			fl=fh.readlines()
		print(fl)
		print(f'file : {fl}')
		time.sleep(5)
		print(f'file end :{fl}')



print('start')
obj1=base("pdfout1.txt");obj2=base("pdfout2.txt");
with concurrent.futures.ThreadPoolExecutor() as exe:
	exe.submit(obj1.process)
	exe.submit(obj2.process)
for r in concurrent.futures.results():
	print(r)
print('end')

