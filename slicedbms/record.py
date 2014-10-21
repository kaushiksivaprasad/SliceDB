
class SliceRecord:
    def __init__(self,db,index,restriction = None):
        from slicedbms.db import SliceDb
        global SliceDb
        self.__db = db
        self.__index = index
        self.stringContents = {}
        self.intContents = {}
        self.doubleContents = {}
        self.primaryKeyVal = None
        self.restriction = restriction
    def getInt(self,colName):
        if((colName not in self.__db.schema.columns.keys() or self.__db.schema.columns[colName] != SliceDb.INT) or (self.restriction and colName not in self.restriction)):
            raise Exception("The given column is not an Integer or it doesn't exist..")
        else:
            return int((self.__db.dataList[self.__index])[colName])
    def getString(self,colName):
        if(colName not in self.__db.schema.columns or self.__db.schema.columns[colName] != SliceDb.STRING or (self.restriction and colName not in self.restriction)):
            raise Exception("The given column is not a String or it doesn't exist..")
        else:
            return str((self.__db.dataList[self.__index])[colName])
    def getDouble(self,colName):
        if(colName not in self.__db.schema.columns or self.__db.schema.columns[colName] != SliceDb.DOUBLE or (self.restriction and colName not in self.restriction)):
            raise Exception("The given column is not a Double or it doesn't exist..")
        else:
            return float((self.__db.dataList[self.__index])[colName])
    def setString(self, colName, data):
        if(colName not in self.__db.schema.columns and self.__db.schema.columns[colName] != SliceDb.STRING):
            raise Exception("The given column is not a String or it doesn't exist..")
        else:
            try:
                int(data)
                raise Exception("The data is not a string..")
            except ValueError:
                if colName == self.__db.schema.primaryKey:
                    self.primaryKeyVal = str(data)
                self.stringContents[colName] = data
            
            
    def setInt(self, colName, data):
        if(colName not in self.__db.schema.columns and self.__db.schema.columns[colName] != SliceDb.INT):
            raise Exception("The given column is not an Integer or it doesn't exist..")
        else:
            try:
                data = int(data)                
            except ValueError:
                raise Exception("The data is not a int..")
            if colName == self.__db.schema.primaryKey:
                self.primaryKeyVal = str(data)
            self.intContents[colName] = data
            
    def setDouble(self, colName, data):
        if(colName not in self.__db.schema.columns and self.__db.schema.columns[colName] != SliceDb.STRING):
            raise Exception("The given column is not a Double or it doesn't exist..")
        else:
            try:
                data = float(data)
            except ValueError:
                raise Exception("The data is not a string..")
            if colName == self.__db.schema.primaryKey:
                self.primaryKeyVal = str(data)
            self.doubleContents[colName] = data
                  
            
        
