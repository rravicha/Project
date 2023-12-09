class Property():                                                                   #creating a class property which will contain all data of properties and their respective functions

    def __init__(self,name,color,country,locx,locy,cost,x1,y1,x2,y2):              #initialising every object(property) with its basic information
        self.name = name
        self.color = color
        self.country = country
        self.locx = locx
        self.locy = locy
        self.cost = cost
        self.houses = [0.1*self.cost,0.4*self.cost,0.5*self.cost,0.6*self.cost,self.cost]    #A list keeping track of rents to be paid v/s no. of houses
        self.mortgage = 0.4*self.cost
        self.mort = 0
        self.no_of_houses = 0
        self.owner = None
        self.x1 = x1
        self.x2 = x2
        self.y1 = y1
        self.y2 = y2