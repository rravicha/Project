import sys
best={}
while(True):
    
    qty=int(input("qty:"))
    if qty==0:
        print("end ")
        sorted_by_value = sorted(best.items(), key=lambda kv: kv[1])
        print(max(sorted_by_value))
        sys.exit()
    prz=int(input("price:"))

    per=prz/qty
    best[qty]=per


    

