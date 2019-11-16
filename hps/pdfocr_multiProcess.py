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
import threading
import multiprocessing
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
	a=time.time()
	obj1=pdfocr("sample1.pdf","pdfout1.txt")
	obj2=pdfocr("sample2.pdf","pdfout2.txt")
	# obj1.process()
	# obj2.process()
	thread1=threading.Thread(target=obj1.process)
	thread2=threading.Thread(target=obj2.process)
	# thread1=multiprocessing.Process(target=obj1.process)
	# thread2=multiprocessing.Process(target=obj2.process)
	thread1.start()
	thread2.start()
	thread1.join()
	thread2.join()
	b=time.time()
	print(f'Time Taken -> {b-a}')





