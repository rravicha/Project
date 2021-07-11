s=[(0, 'a'), (1, 'b'), (2, 'c'), (3, 'd'), (4, 'e'), (5, 'f'), (6, 'g'), (7, 'h'), (8, 'i'), (9, 'j'), (10, 'k'), (11, 'l'), (12, 'm'), (13, 'n'), (14, 'o'), (15, 'p'), (16, 'q'), (17, 'r'), (18, 's'), (19, 't'), (20, 'u'), (21, 'v'), (22, 'w'), (23, 'x'), (24, 'y'), (25, 'z')]
out=[]

def fetch(num):
    for i in s:
        if i[0]==num:
            return (i[1])

inp=input("num")
inp='-'.join(inp)
inp=list(inp.split('-'))
inp=[int(i) for i in inp]


for i in inp:
    out.append(fetch(i))

out=''.join(out)

print(out[0:3]+out[3:6]+out[6:])
print('-----')

def fetch(num):
    for i in s:
        if i[1]==num:
            return (i[0])

inp=input("trick")
inp='-'.join(inp)
inp=list(inp.split('-'))
inp=[str(i) for i in inp]
out=[]

for i in inp:
    out.append(fetch(i))

out=''.join(out)

print(out[0:3]+out[3:6]+out[6:])
print('-----')
