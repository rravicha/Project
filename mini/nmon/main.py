from faker import Faker
import time
class User:
        
    def __init__(self):

        self.f=Faker()

    def auto_generate(self):
        self.name = str(self.f.name())
        self.email = str(self.f.email())
        self.date_of_birth = str(self.f.date_of_birth())
        self.address = str(self.f.address())
        self.country = str(self.f.country())
        self.state = str(self.f.state())
        self.city  = str(self.f.city())
        self.account_id = str(self.f.random_number())
        return self

if __name__=="__main__":
    for x in range(567):
        u=User()
        obj=u.auto_generate()
        print(f"secret agent information - personal details - displaying {x}/567")
        print(f"name :{obj.name}")
        print(f"email :{obj.email}")
        print(f"date_of_birth :{obj.date_of_birth}")
        print(f"address :{obj.address}")
        print(f"country :{obj.country}")
        print(f"state :{obj.state}")
        print(f"city :{obj.city}")
        print(f"account_id :{obj.account_id}")
        time.sleep(5)
        print('*'*35)