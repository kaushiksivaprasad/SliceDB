
class SliceSchema:
    def __init__(self,name):
        self.name = None
        self.primaryKey = None
        self.columns = dict()
        self.columnOrder = ""
    def setColumnInfo(self,columnName,columnType,primaryKey):
        if(not self.primaryKey and primaryKey):
            self.primaryKey = columnName
        self.columns[columnName] = columnType
        if(self.columnOrder != ""):
            self.columnOrder = self.columnOrder+","+columnName
        else:
            self.columnOrder = columnName
