from constants import gameConstants as gc
''''
Assets are defined here
'''
class warehouse:
    def __init__(self,cap):
        self.cap=cap
    def add_cap(self,num):
        self.cap = self.cap + num
    def rem_cap(self,num):
        self.cap = self.cap - num
    def get_cap(self):
        return self.cap

class player:
    def __init__(self, name, symbol, balance, status='Active'):
        self.name=name
        self.symbol=symbol
        self.balance=gc.PLAYER_INIT_AMT
        self.status=status
    
    