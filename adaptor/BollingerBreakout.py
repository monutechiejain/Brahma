
import backtrader  as bt

import saksham_bt_ind as indlib

class BollingerBreakout(bt.Strategy):
    # list of parameters which are configurable for the strategy
    params = dict(
        bollingerLengths=20
    )

    alias = ("BRAHMA", "BollingerBreakout")

    def __init__(self):
        self.boll = indlib.bollinger.BollingerBandwidth(period=self.p.bollingerLengths, devfactor=2.0) 

        # self.boll = indlib.BBbandwidth(period=self.p.bollingerLengths, devfactor=2.0) # Shorthand for direct indicator access

        
        self.upBand = self.boll.lines.top
        self.dnBand = self.boll.lines.bot
        self.midBand = self.boll.lines.mid
        self.bbbandwidth =  self.boll.lines.bbbandwidth


    def next(self):
        pass
    
