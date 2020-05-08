import wikipedia

res=wikipedia.search(input("Seach:"))
# print(res)
# page=wikipedia.page(res[0])
# print(page.content)

for i,r in enumerate(res):
    print(i+1,'  ',r)

choice=int(input("Please choose any option to brief : "))

# print(wikipedia.page(res[res(int(choice))].content))

page=wikipedia.page(res[choice])

print(page.content)