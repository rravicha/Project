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
# import time
import io
from PIL import Image
import pytesseract as pt
from wand.image import Image as wi
import concurrent.futures
import glob
import os
# import time
import logging

lf='%(asctime)s %(levelname)s %(message)s'
logging.basicConfig(filename="log.dat",filemode='w',format=lf,level=logging.INFO)
l=logging.getLogger()
class pdfocr:
	def __init__(self,flist):
		l.info("flist"+flist)
		self.flist=flist
		print(f'flist%%%%%%%%%%%%%%%:{self.flist}')
		print(type(flist))
		for i in range(len(self.flist)):
			self.infile=self.flist[i]
			self.outfile=self.infile.split('.')[0]+'.txt'
			print(f'processing file : {self.infile} --> {self.outfile}')
			self.process()
	def process(self):
		print(f'{self.infile} ->initializing....')
		self.pdf=wi(filename=self.infile,resolution=300)
		self.pdi=self.pdf.convert('jpeg')
		self.il=[]
		print(f'{self.infile} ->sequencing....')
		for img in self.pdi.sequence:
		    self.i=wi(image=img)
		    self.il.append(self.i.make_blob('jpeg'))
		
		self.t=[]
		print(f'{self.infile} ->extract...')
		for item in self.il:
		    self.I=Image.open(io.BytesIO(item))
		    self.text=pt.image_to_string(self.I,lang="eng")
		    self.t.append(self.text)
		print(f'{self.infile} ->writing....')
		with open(self.outfile,'w') as fh:
			# os.system("tail -f {self.outfile}")
		    for line in self.t:
		        fh.write(line)
		print(f'{self.infile} ->END!')

if __name__=='__main__':
	l.info('!!start')
	file_list=[]
	os.chdir('container')
	
	files=glob.glob('*.pdf')
	# obj1=pdfocr(files)

	with concurrent.futures.ThreadPoolExecutor() as execute:
		files=glob.glob('*.pdf')
		# print(f'Files:{files}')
		# l.info("files %s",files)
		# results=execute.map(pdfocr,files)
		obj1=execute.map(pdfocr,files)
		# execute.submit(obj1.process)
	# for result in results:
		# print(result)
	print('!!start')

# '''
# test


# class test:
# 	def __init__(self,exe,f):
# 		self.e=exe
# 		self.f=f
# 		self.e.submit(f)
# 	def process(self,i):
# 		self.each_file=i
# 		print(f'processing file {self.each_file}')
# 		time.sleep(5)
# 		print(f'sleep over for file {self.each_file}')
# '''