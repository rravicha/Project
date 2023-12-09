def wait_for_option():
    option=int(input("Option >>"))
    return option
def inventory():
    print('''
1) View Inventory
2) Edit Inventory
3) Generate Report
          ''')
    opt=wait_for_option()

import os;os.system('cls')
while True:
    print('''
1) Inventory
2) Sales
3) Employee Management
4) Truck Management
5) Departments
6) Parking Control
          ''')
    opt=wait_for_option()
    if opt==1:
        inventory()
