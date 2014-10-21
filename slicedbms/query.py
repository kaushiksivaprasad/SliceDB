class SliceQuery:
    def __init__(self,columns,condition):
        if(all(isinstance(a, basestring) for a in columns) 
           and isinstance(condition, SliceCondition)):
            self.columns = columns
            self.condition = condition
        else:
            raise Exception("Invalid parameters..")
            
        
class SliceOp:
    EQ = "EQ"
    GT = "GT"
    LT = "LT"
class SliceCondition:
    def addColumn(self,column):
        self.column = column
    def addOp(self, op):
            self.op = op
    def addLiteral(self,literal):
        self.literal = literal