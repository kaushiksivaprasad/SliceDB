
from slicedbms.db import SliceDb
class SliceField:
    def __init__(self,name,type):
        if(isinstance(name, basestring) and isinstance(type,basestring) and type == SliceDb.INT or type == SliceDb.STRING
           or type == SliceDb.DOUBLE):
            self.name = name
            self.type = type
        else:
            raise Exception("Wrong parameters passed..")
