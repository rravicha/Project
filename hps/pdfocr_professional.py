# -*- coding: utf-8 -*-
import io
from PIL import Image
import pytesseract as pt
from wand.image import Image as wi

pdf=wi(filename="sample2.pdf",resolution=300)
pdi=pdf.convert('jpeg')

il=[]

for img in pdi.sequence:
    i=wi(image=img)
    il.append(i.make_blob('jpeg'))

t=[]

for item in il:
    I=Image.open(io.BytesIO(item))
    text=pt.image_to_string(I,lang="eng")
    t.append(text)

print('____________________________________')
print(t)
print('____________________________________')

with open("pdfout.txt",'w') as fh:
    for line in t:
        fh.write(line)






'''
##
    no of words 2 3 4
    #
'''