class Player:
    def __init__(self,color,no):  #each player initialised with its data
        self.cash = 300000
        self.posx = display_height-card_length/2
        self.posy = display_height-card_length/2
        self.total_wealth = 300000
        self.properties = []
        self.getoutofjailcard = 0
        self.color = color
        self.no = no
        self.no_of_railways = 0
        self.released = 1