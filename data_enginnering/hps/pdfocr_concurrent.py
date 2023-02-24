'''
Load Testing with 2 files
manual takes 39.65,38.86,42.64
mprocess 13.94,13.51
thread 13.71
'''

# -*- coding: utf-8 -*-
import time
import io
from PIL import Image
import pytesseract as pt
from wand.image import Image as wi
import concurrent.futures
class pdfocr:
	def __init__(self,filein,fileout):
		self.infile=filein
		self.outfile=fileout
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
		    for line in self.t:
		        fh.write(line)
		print(f'{self.infile} ->END!')


if __name__=='__main__':
	
	obj1=pdfocr("sample1.pdf","pdfout1.txt")
	obj2=pdfocr("sample2.pdf","pdfout2.txt")
	a=time.time()
	with concurrent.futures.ProcessPoolExecutor() as exe:
	# with concurrent.futures.ThreadPoolExecutor() as exe:
		exe.submit(obj1.process)
		exe.submit(obj2.process)
	# for r in concurrent.futures.as_completed(exe):
		# print(r.result())
	b=time.time()

	print(f'Time Taken -> {b-a}')
	c=time.time()
	# with concurrent.futures.ProcessPoolExecutor() as exe:
	with concurrent.futures.ThreadPoolExecutor() as exe:
		exe.submit(obj1.process)
		exe.submit(obj2.process)
	# for r in concurrent.futures.as_completed(exe):
		# print(r.result())
	d=time.time()
	
	print(f'Time Taken -> {d-c}')




#thread -13.80
#proces -19.31
#together